"""
Glue ETL Job: Orders

Reads orders CSV files from the S3 raw zone, validates, deduplicates, upserts
into a Delta Lake table partitioned by date, and archives the sources.

Job parameters:
  --S3_BUCKET, --RAW_PREFIX, --DWH_PREFIX, --ARCHIVED_PREFIX,
  --REJECTED_PREFIX, --JOB_NAME
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.etl import DatasetConfig, run_dataset_etl

# order_timestamp and date are read as strings and cast explicitly, so an
# unparseable value becomes a null we can reject with an accurate reason
# rather than a silent CSV-parser null.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", IntegerType(), nullable=True),
        StructField("order_id", LongType(), nullable=True),
        StructField("user_id", LongType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("date", StringType(), nullable=True),
    ]
)


def build_config(args: dict) -> DatasetConfig:
    bucket = args["S3_BUCKET"]
    return DatasetConfig(
        name="orders",
        bucket=bucket,
        raw_prefix=args["RAW_PREFIX"],
        dwh_path=f"s3://{bucket}/{args['DWH_PREFIX']}",
        archived_prefix=args["ARCHIVED_PREFIX"],
        rejected_path=f"s3://{bucket}/{args['REJECTED_PREFIX']}",
        schema=ORDERS_SCHEMA,
        pk_col="order_id",
        required_cols=["user_id"],
        casts={"order_timestamp": F.to_timestamp, "date": F.to_date},
        timestamp_col="order_timestamp",
        partition_col="date",
        order_col="order_timestamp",
        max_rejection_rate=float(args.get("MAX_REJECTION_RATE", 0.05)),
    )


def main(spark, args: dict, run_id: str = "local") -> dict:
    return run_dataset_etl(spark, build_config(args), run_id)


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
        ],
    )

    glue_context = GlueContext(SparkContext())
    glue_job = Job(glue_context)
    glue_job.init(job_args["JOB_NAME"], job_args)

    main(glue_context.spark_session, job_args, resolve_run_id(sys.argv))

    glue_job.commit()
