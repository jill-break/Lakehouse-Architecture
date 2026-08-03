"""
Shared utilities for all Glue ETL jobs.

Covers raw-zone listing, validation, deduplication, Delta merge helpers,
rejected-record logging and S3 archiving. Everything here is pure Spark or
boto3 so it can be unit-tested locally without a Glue runtime.
"""

from __future__ import annotations

from datetime import datetime, timezone

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CSV only. The reader is spark.read.csv, so listing a leftover .xlsx would just
# hand the CSV parser a zip archive and fail the run on a confusing header error.
RAW_SUFFIXES = (".csv",)


# ---------------------------------------------------------------------------
# Raw zone listing
# ---------------------------------------------------------------------------


def list_raw_keys(
    bucket: str, prefix: str, suffixes: tuple[str, ...] = RAW_SUFFIXES
) -> list[str]:
    """
    List the data objects currently under a raw prefix.

    Every job lists once and then reads *and* archives exactly this set of keys.
    That closes the read/archive race: a file that lands after this call is left
    in the raw zone for the next run rather than being archived unread.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(suffixes) and obj["Size"] > 0
    ]


# ---------------------------------------------------------------------------
# Validation helpers
#
# Each helper splits a DataFrame into (valid, rejected) and threads the
# accumulated rejected frame through, so no rejected row is ever dropped.
# ---------------------------------------------------------------------------


def _accumulate(rejected: DataFrame, new_rejected: DataFrame) -> DataFrame:
    """Union rejected frames by column name, tolerating the first (None) call."""
    if rejected is None:
        return new_rejected
    return rejected.unionByName(new_rejected, allowMissingColumns=True)


def drop_null_pk(
    df: DataFrame, pk_col: str, rejected: DataFrame, reason: str
) -> tuple[DataFrame, DataFrame]:
    """Split df into valid (non-null pk) and rejected rows."""
    null_mask = F.col(pk_col).isNull()
    new_rejected = df.filter(null_mask).withColumn("rejection_reason", F.lit(reason))
    return df.filter(~null_mask), _accumulate(rejected, new_rejected)


def drop_null_cols(
    df: DataFrame, cols: list[str], rejected: DataFrame, reason: str = None
) -> tuple[DataFrame, DataFrame]:
    """Reject rows where any of the given columns are null."""
    mask = F.lit(False)
    for c in cols:
        mask = mask | F.col(c).isNull()
    reason = reason or f"null value in required column(s): {cols}"
    new_rejected = df.filter(mask).withColumn("rejection_reason", F.lit(reason))
    return df.filter(~mask), _accumulate(rejected, new_rejected)


def validate_timestamps(
    df: DataFrame, ts_col: str, rejected: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Reject rows whose timestamp column is null or unparseable.

    Assumes ts_col has already been cast with to_timestamp/to_date — an
    unparseable source value casts to null, which is what this rejects.
    """
    invalid_mask = F.col(ts_col).isNull()
    reason = f"null or unparseable timestamp in column: {ts_col}"
    new_rejected = df.filter(invalid_mask).withColumn("rejection_reason", F.lit(reason))
    return df.filter(~invalid_mask), _accumulate(rejected, new_rejected)


def reject_orphans(
    df: DataFrame,
    reference_df: DataFrame,
    key_col: str,
    rejected: DataFrame,
    reason: str,
) -> tuple[DataFrame, DataFrame]:
    """
    Referential integrity via distributed anti/semi joins.

    reference_df must be a single-column frame of valid keys. Nothing is
    collected to the driver, so the check stays O(cluster) rather than
    O(driver memory) as the referenced table grows.
    """
    columns = df.columns  # a join on a column name reorders it to the front
    keys = reference_df.select(F.col(key_col).alias(key_col)).distinct()

    orphans = (
        df.join(keys, on=key_col, how="left_anti")
        .select(*columns)
        .withColumn("rejection_reason", F.lit(reason))
    )
    valid = df.join(keys, on=key_col, how="left_semi").select(*columns)
    return valid, _accumulate(rejected, orphans)


def deduplicate(df: DataFrame, pk_col: str, order_col: str = None) -> DataFrame:
    """
    Keep exactly one row per pk_col.

    Rows are ranked by order_col descending when given, then by a hash of the
    whole row. The hash tiebreak makes the survivor deterministic when two rows
    share a primary key *and* an identical timestamp — common in replayed or
    re-exported batches, where an arbitrary winner would make consecutive runs
    disagree.
    """
    row_hash = F.hash(F.struct(*[F.col(c) for c in df.columns]))
    ordering = [F.col(order_col).desc_nulls_last()] if order_col else []
    ordering.append(row_hash.desc())

    w = Window.partitionBy(pk_col).orderBy(*ordering)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Delta Lake helpers
# ---------------------------------------------------------------------------


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    delta_path: str,
    merge_key: str,
    partition_col: str = None,
) -> None:
    """
    Idempotently merge source_df into the Delta table at delta_path, creating
    the table (optionally partitioned) on first run.

    The merge condition is the primary key *alone*. Including the partition
    column would be faster but is wrong twice over:

      * NULL = NULL is UNKNOWN in SQL, so a row with a null partition value
        could never match its existing target row and would be re-inserted on
        every run — unbounded duplication of exactly the dirty rows this
        pipeline exists to catch.
      * When a record legitimately changes partition (a product is reclassified,
        an order date is corrected) the target row lives elsewhere, so the merge
        would insert a second row with the same primary key.

    Callers reject null partition values before merging, so the null case is
    defence in depth; the mutable-partition case is real and this is the fix.
    """
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        (
            delta_table.alias("target")
            .merge(
                source_df.alias("source"),
                f"target.{merge_key} = source.{merge_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(delta_path)


# ---------------------------------------------------------------------------
# Rejected records writer
# ---------------------------------------------------------------------------


def write_rejected(
    df: DataFrame, rejected_path: str, job_name: str, run_id: str = "local"
) -> int:
    """
    Persist rejected records as Parquet and return how many there were.

    The frame is cached and counted once — the previous implementation walked
    the lineage three times (isEmpty, write, count), re-reading S3 each time.
    The path carries the Glue run id as well as a timestamp, and the write
    appends, so two batches in the same second cannot clobber each other.
    """
    if df is None:
        return 0

    df = df.cache()
    count = df.count()
    if count == 0:
        df.unpersist()
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_path = f"{rejected_path}/{job_name}/{ts}_{run_id}"
    df.write.mode("append").parquet(out_path)
    df.unpersist()
    return count


# ---------------------------------------------------------------------------
# Data quality gate
# ---------------------------------------------------------------------------


class DataQualityError(Exception):
    """Raised when the share of rejected rows exceeds the configured ceiling."""


def check_rejection_threshold(
    total_rows: int, rejected_rows: int, max_rejection_rate: float
) -> float:
    """
    Circuit breaker. Counting rejects without ever acting on the count means a
    fully malformed upstream export produces an empty merge, a green pipeline
    and no alert. Returns the rejection rate; raises above the ceiling.
    """
    if total_rows <= 0:
        return 0.0

    rate = rejected_rows / total_rows
    if rate > max_rejection_rate:
        raise DataQualityError(
            f"rejection rate {rate:.1%} exceeds the maximum of "
            f"{max_rejection_rate:.1%} ({rejected_rows}/{total_rows} rows rejected)"
        )
    return rate


# ---------------------------------------------------------------------------
# S3 archive helper
# ---------------------------------------------------------------------------


def archive_s3_object(bucket: str, source_key: str, archived_prefix: str) -> None:
    """
    Copy a raw S3 object to the archived prefix then delete the original.
    Skips folder placeholder objects (keys ending with '/').
    source_key example: raw/orders/orders_apr_2025.csv
    """
    if source_key.endswith("/"):
        return

    filename = source_key.split("/")[-1]
    if not filename:
        return

    s3 = boto3.client("s3")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    dest_key = f"{archived_prefix}/{ts}_{filename}"

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=bucket, Key=source_key)
