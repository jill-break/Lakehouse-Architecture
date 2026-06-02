"""
Shared utilities for all Glue ETL jobs.
Covers schema validation, rejection logging, Delta merge helpers, and S3 archiving.
"""

import boto3
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def drop_null_pk(
    df: DataFrame, pk_col: str, rejected: DataFrame, reason: str
) -> tuple[DataFrame, DataFrame]:
    """Split df into valid (non-null pk) and rejected rows."""
    null_mask = F.col(pk_col).isNull()
    new_rejected = df.filter(null_mask).withColumn("rejection_reason", F.lit(reason))
    valid = df.filter(~null_mask)
    return valid, rejected.union(new_rejected) if rejected is not None else new_rejected


def drop_null_cols(
    df: DataFrame, cols: list[str], rejected: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Reject rows where any of the given columns are null."""
    mask = F.lit(False)
    for c in cols:
        mask = mask | F.col(c).isNull()
    reason = f"null value in required column(s): {cols}"
    new_rejected = df.filter(mask).withColumn("rejection_reason", F.lit(reason))
    valid = df.filter(~mask)
    return valid, rejected.union(new_rejected) if rejected is not None else new_rejected


def validate_timestamps(
    df: DataFrame, ts_col: str, rejected: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Reject rows where the timestamp column cannot be parsed."""
    parsed = df.withColumn("_ts_check", F.to_timestamp(F.col(ts_col)))
    invalid_mask = F.col("_ts_check").isNull()
    reason = f"invalid or unparseable timestamp in column: {ts_col}"
    new_rejected = (
        parsed.filter(invalid_mask)
        .drop("_ts_check")
        .withColumn("rejection_reason", F.lit(reason))
    )
    valid = parsed.filter(~invalid_mask).drop("_ts_check")
    return valid, rejected.union(new_rejected) if rejected is not None else new_rejected


def deduplicate(df: DataFrame, pk_col: str, order_col: str = None) -> DataFrame:
    """Deduplicate on pk_col, keeping the row with the latest order_col value."""
    from pyspark.sql.window import Window

    if order_col:
        w = Window.partitionBy(pk_col).orderBy(F.col(order_col).desc())
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    return df.dropDuplicates([pk_col])


# ---------------------------------------------------------------------------
# Delta Lake helpers
# ---------------------------------------------------------------------------


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    delta_path: str,
    merge_key: str,
    schema: StructType = None,
) -> None:
    """
    Merge source_df into an existing Delta table at delta_path.
    Creates the table on first run if it does not exist.
    """
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            source_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        # First-time write — create the Delta table
        writer = source_df.write.format("delta").mode("overwrite")
        if schema:
            writer = writer.option("overwriteSchema", "true")
        writer.save(delta_path)


def upsert_to_delta_partitioned(
    spark: SparkSession,
    source_df: DataFrame,
    delta_path: str,
    merge_key: str,
    partition_col: str,
    schema: StructType = None,
) -> None:
    """Merge with partition pruning for better performance on large tables."""
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            source_df.alias("source"),
            f"target.{partition_col} = source.{partition_col}"
            f" AND target.{merge_key} = source.{merge_key}",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .partitionBy(partition_col)
            .option("overwriteSchema", "true")
            .save(delta_path)
        )


# ---------------------------------------------------------------------------
# Rejected records writer
# ---------------------------------------------------------------------------


def write_rejected(df: DataFrame, rejected_path: str, job_name: str) -> None:
    """Write rejected records to the rejected zone as Parquet with a timestamp prefix."""  # noqa: E501
    if df is None or df.rdd.isEmpty():
        return
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_path = f"{rejected_path}/{job_name}/{ts}"
    df.write.mode("overwrite").parquet(out_path)
    print(f"[{job_name}] Wrote {df.count()} rejected records to {out_path}")


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

    s3 = boto3.client("s3")
    filename = source_key.split("/")[-1]
    if not filename:
        return

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    dest_key = f"{archived_prefix}/{ts}_{filename}"

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=bucket, Key=source_key)
    print(f"Archived s3://{bucket}/{source_key} -> s3://{bucket}/{dest_key}")


# ---------------------------------------------------------------------------
# Glue job args helper
# ---------------------------------------------------------------------------


def get_job_args(required_keys: list[str]) -> dict:
    """Parse Glue job arguments and validate that all required keys are present."""
    from awsglue.utils import getResolvedOptions
    import sys

    args = getResolvedOptions(sys.argv, required_keys)
    return args
