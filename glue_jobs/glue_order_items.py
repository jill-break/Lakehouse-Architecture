"""
Glue ETL Job: Order Items

Reads order_items CSV files from the S3 raw zone, validates (including
referential integrity against both the orders and the products Delta tables),
deduplicates, upserts into a Delta Lake table partitioned by date, and archives
the sources.

Job parameters:
  --S3_BUCKET, --RAW_PREFIX, --DWH_PREFIX, --ARCHIVED_PREFIX,
  --REJECTED_PREFIX, --ORDERS_DWH_PREFIX, --PRODUCTS_DWH_PREFIX, --JOB_NAME
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.etl import DatasetConfig, run_dataset_etl
from common.logging_utils import get_logger
from common.utils import reject_orphans

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=True),
        StructField("order_id", LongType(), nullable=True),
        StructField("user_id", LongType(), nullable=True),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
        StructField("product_id", LongType(), nullable=True),
        StructField("add_to_cart_order", IntegerType(), nullable=True),
        StructField("reordered", IntegerType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("date", StringType(), nullable=True),
    ]
)


def build_config(args: dict) -> DatasetConfig:
    bucket = args["S3_BUCKET"]
    return DatasetConfig(
        name="order_items",
        bucket=bucket,
        raw_prefix=args["RAW_PREFIX"],
        dwh_path=f"s3://{bucket}/{args['DWH_PREFIX']}",
        archived_prefix=args["ARCHIVED_PREFIX"],
        rejected_path=f"s3://{bucket}/{args['REJECTED_PREFIX']}",
        schema=ORDER_ITEMS_SCHEMA,
        pk_col="id",
        required_cols=["order_id", "user_id"],
        casts={"order_timestamp": F.to_timestamp, "date": F.to_date},
        timestamp_col="order_timestamp",
        partition_col="date",
        order_col="order_timestamp",
        max_rejection_rate=float(args.get("MAX_REJECTION_RATE", 0.05)),
    )


def make_fk_hook(delta_path: str, key_col: str, table: str):
    """
    Build a referential-integrity validation hook against a Delta dimension.

    The check is a distributed anti/semi join, never a driver-side collect: the
    referenced table can grow to millions of keys without the driver noticing.
    """
    reason = f"{key_col} not found in {table} table (referential integrity)"

    def hook(spark, df, rejected):
        log = get_logger("order_items")
        if not DeltaTable.isDeltaTable(spark, delta_path):
            log.warning(
                "referential_integrity_skipped",
                extra={"table": table, "path": delta_path, "reason": "table missing"},
            )
            return df, rejected

        reference = spark.read.format("delta").load(delta_path).select(key_col)
        valid, rejected = reject_orphans(df, reference, key_col, rejected, reason)
        log.info("referential_integrity_checked", extra={"table": table})
        return valid, rejected

    return hook


def build_validations(args: dict) -> list:
    bucket = args["S3_BUCKET"]
    return [
        make_fk_hook(
            f"s3://{bucket}/{args['ORDERS_DWH_PREFIX']}", "order_id", "orders"
        ),
        make_fk_hook(
            f"s3://{bucket}/{args['PRODUCTS_DWH_PREFIX']}", "product_id", "products"
        ),
    ]


def main(spark, args: dict, run_id: str = "local") -> dict:
    return run_dataset_etl(
        spark, build_config(args), run_id, extra_validations=build_validations(args)
    )


if __name__ == "__main__":
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    from common.logging_utils import resolve_run_id

    job_args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "S3_BUCKET",
            "RAW_PREFIX",
            "DWH_PREFIX",
            "ARCHIVED_PREFIX",
            "REJECTED_PREFIX",
            "ORDERS_DWH_PREFIX",
            "PRODUCTS_DWH_PREFIX",
        ],
    )

    glue_context = GlueContext(SparkContext())
    glue_job = Job(glue_context)
    glue_job.init(job_args["JOB_NAME"], job_args)

    main(glue_context.spark_session, job_args, resolve_run_id(sys.argv))

    glue_job.commit()
