"""
Config-driven ETL runner shared by every dataset.

The three ETL jobs previously carried a near-identical copy of the same
list -> read -> union -> cast -> validate -> dedup -> merge -> reject -> archive
sequence. That sequence lives here once; each job supplies a DatasetConfig and
any dataset-specific validation hooks.

The pieces are split so the pure Spark logic (`transform`) can be unit-tested
without S3 or a Glue runtime, while `run_dataset_etl` wires in the IO.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common.logging_utils import emit_metric, get_logger
from common.utils import (
    archive_s3_object,
    check_rejection_threshold,
    deduplicate,
    drop_null_cols,
    drop_null_pk,
    list_raw_keys,
    upsert_to_delta,
    validate_timestamps,
    write_rejected,
)

# A validation hook: (spark, valid_df, rejected_df) -> (valid_df, rejected_df)
ValidationHook = Callable[
    [SparkSession, DataFrame, Optional[DataFrame]],
    Tuple[DataFrame, Optional[DataFrame]],
]


@dataclass
class DatasetConfig:
    """Everything that differs between the products, orders and order_items jobs."""

    name: str
    bucket: str
    raw_prefix: str
    dwh_path: str
    archived_prefix: str
    rejected_path: str
    schema: StructType
    pk_col: str
    required_cols: List[str] = field(default_factory=list)
    # column name -> function applied to that column after the raw read
    casts: Dict[str, Callable] = field(default_factory=dict)
    timestamp_col: Optional[str] = None
    partition_col: Optional[str] = None
    order_col: Optional[str] = None
    max_rejection_rate: float = 0.05


def read_paths(spark: SparkSession, schema: StructType, paths: List[str]) -> DataFrame:
    """
    Read CSV paths natively with Spark, parallelised across executors.

    `enforceSchema=false` makes Spark validate the CSV header against the
    declared schema instead of binding columns by position, so a file whose
    columns are reordered or renamed fails loudly rather than quietly loading
    department strings into an integer id.

    Rows that fail type coercion become nulls (PERMISSIVE) and are caught by
    the null-primary-key and required-column checks, which route them to the
    rejected zone with a reason attached.
    """
    return (
        spark.read.option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(paths)
    )


def read_raw(spark: SparkSession, config: DatasetConfig, keys: List[str]) -> DataFrame:
    """Read the given raw S3 keys for this dataset."""
    return read_paths(
        spark, config.schema, [f"s3://{config.bucket}/{key}" for key in keys]
    )


def transform(
    spark: SparkSession,
    raw_df: DataFrame,
    config: DatasetConfig,
    extra_validations: List[ValidationHook] = (),
) -> Tuple[DataFrame, Optional[DataFrame], Dict[str, int]]:
    """
    Cast, validate and deduplicate a raw frame.

    Returns (valid_df, rejected_df, stats). Pure Spark — no S3, no Glue.
    """
    df = raw_df
    for column, cast_fn in config.casts.items():
        df = df.withColumn(column, cast_fn(F.col(column)))

    # Cache once after casting: the count below, every validation filter and the
    # merge all share this lineage, which would otherwise be recomputed (and the
    # source re-read from S3) on each action.
    cast_df = df.cache()
    df = cast_df
    total_rows = df.count()

    rejected: Optional[DataFrame] = None

    df, rejected = drop_null_pk(
        df, config.pk_col, rejected, f"null {config.pk_col} (primary key)"
    )

    if config.required_cols:
        df, rejected = drop_null_cols(df, config.required_cols, rejected)

    if config.timestamp_col:
        df, rejected = validate_timestamps(df, config.timestamp_col, rejected)

    # A null partition value would land rows in __HIVE_DEFAULT_PARTITION__ and
    # is almost always an upstream parsing failure — reject it explicitly.
    if config.partition_col and config.partition_col not in config.required_cols:
        df, rejected = drop_null_cols(
            df,
            [config.partition_col],
            rejected,
            f"null partition column: {config.partition_col}",
        )

    for hook in extra_validations:
        df, rejected = hook(spark, df, rejected)

    df = deduplicate(df, pk_col=config.pk_col, order_col=config.order_col)
    df = df.withColumn("ingested_at", F.current_timestamp())
    df = df.cache()

    # cast_df stays cached until the caller has written both the merge and the
    # rejected records — both still depend on this lineage.
    valid_rows = df.count()
    rejected_rows = rejected.count() if rejected is not None else 0

    stats = {
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "rejected_rows": rejected_rows,
        "duplicate_rows": max(total_rows - rejected_rows - valid_rows, 0),
    }
    return df, rejected, stats


def run_dataset_etl(
    spark: SparkSession,
    config: DatasetConfig,
    run_id: str = "local",
    extra_validations: List[ValidationHook] = (),
) -> Dict:
    """Run the full ETL for one dataset and return a summary dict."""
    log = get_logger(config.name, run_id)

    # List once; read and archive exactly these keys. Anything that lands
    # mid-run stays in the raw zone for the next execution instead of being
    # archived without ever being ingested.
    keys = list_raw_keys(config.bucket, config.raw_prefix)
    if not keys:
        log.info(
            "no_raw_files",
            extra={"raw_prefix": config.raw_prefix, "status": "skipped"},
        )
        return {"status": "skipped", "reason": "no raw files", **_zero_stats()}

    log.info("reading_raw", extra={"file_count": len(keys), "keys": keys})
    raw_df = read_raw(spark, config, keys)

    valid_df, rejected_df, stats = transform(spark, raw_df, config, extra_validations)
    log.info("validation_complete", extra=stats)

    rate = check_rejection_threshold(
        stats["total_rows"], stats["rejected_rows"], config.max_rejection_rate
    )
    log.info("rejection_rate", extra={"rate": round(rate, 4)})

    log.info("upserting", extra={"delta_path": config.dwh_path})
    upsert_to_delta(
        spark=spark,
        source_df=valid_df,
        delta_path=config.dwh_path,
        merge_key=config.pk_col,
        partition_col=config.partition_col,
    )

    written_rejects = write_rejected(
        rejected_df, config.rejected_path, config.name, run_id
    )
    if written_rejects:
        log.warning(
            "rejected_records_written",
            extra={"rows": written_rejects, "path": config.rejected_path},
        )

    # Archive only after the merge has committed.
    for key in keys:
        archive_s3_object(config.bucket, key, config.archived_prefix)
    log.info("archived_source_files", extra={"file_count": len(keys)})

    emit_metric(config.name, "RowsIngested", stats["valid_rows"])
    emit_metric(config.name, "RowsRejected", stats["rejected_rows"])
    emit_metric(config.name, "RejectionRate", rate * 100, unit="Percent")

    valid_df.unpersist()
    log.info("job_complete", extra={"status": "success", **stats})
    return {"status": "success", "files": len(keys), **stats}


def _zero_stats() -> Dict[str, int]:
    return {
        "total_rows": 0,
        "valid_rows": 0,
        "rejected_rows": 0,
        "duplicate_rows": 0,
    }
