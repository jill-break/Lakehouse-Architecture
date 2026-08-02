"""
Glue ETL Job: Products

Reads the products CSV from the S3 raw zone, validates, deduplicates, upserts
into a Delta Lake table and archives the source files.

The table is deliberately *unpartitioned*: 1,000 rows across 7 departments
averages ~143 rows per partition, which costs more in file-listing overhead
than it saves in pruning, and `department` is exactly the kind of attribute
that gets reclassified. Z-ORDER on product_id (see glue_maintenance.py) gives
the lookup performance without the small-file penalty.

Job parameters:
  --S3_BUCKET        e.g. my-lakehouse-bucket
  --RAW_PREFIX       e.g. raw/products
  --DWH_PREFIX       e.g. lakehouse-dwh/products
  --ARCHIVED_PREFIX  e.g. archived/products
  --REJECTED_PREFIX  e.g. rejected/products
  --JOB_NAME         passed automatically by Glue
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common.etl import DatasetConfig, run_dataset_etl

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=True),
        StructField("department_id", IntegerType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
    ]
)


def build_config(args: dict) -> DatasetConfig:
    bucket = args["S3_BUCKET"]
    return DatasetConfig(
        name="products",
        bucket=bucket,
        raw_prefix=args["RAW_PREFIX"],
        dwh_path=f"s3://{bucket}/{args['DWH_PREFIX']}",
        archived_prefix=args["ARCHIVED_PREFIX"],
        rejected_path=f"s3://{bucket}/{args['REJECTED_PREFIX']}",
        schema=PRODUCTS_SCHEMA,
        pk_col="product_id",
        required_cols=["product_name"],
        partition_col=None,
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
