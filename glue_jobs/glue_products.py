"""
Glue ETL Job: Products
Reads products CSV from S3 raw zone, validates, deduplicates,
upserts into a Delta Lake table, and archives the source file.

Job parameters (--key value):
  --S3_BUCKET        e.g. my-lakehouse-bucket
  --RAW_PREFIX       e.g. raw/products
  --DWH_PREFIX       e.g. lakehouse-dwh/products
  --ARCHIVED_PREFIX  e.g. archived/products
  --REJECTED_PREFIX  e.g. rejected/products
  --JOB_NAME         passed automatically by Glue
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
)

from common.utils import (
    drop_null_pk,
    drop_null_cols,
    deduplicate,
    upsert_to_delta,
    write_rejected,
    archive_s3_object,
)

# ---------------------------------------------------------------------------
# Bootstrap Glue / Spark context
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET",
    "RAW_PREFIX",
    "DWH_PREFIX",
    "ARCHIVED_PREFIX",
    "REJECTED_PREFIX",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# Delta Lake config
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

BUCKET = args["S3_BUCKET"]
RAW_PATH = f"s3://{BUCKET}/{args['RAW_PREFIX']}"
DWH_PATH = f"s3://{BUCKET}/{args['DWH_PREFIX']}"
ARCHIVED_PREFIX = args["ARCHIVED_PREFIX"]
REJECTED_PATH = f"s3://{BUCKET}/{args['REJECTED_PREFIX']}"

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",   IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=True),
    StructField("department",   StringType(),  nullable=True),
    StructField("product_name", StringType(),  nullable=True),
])

# ---------------------------------------------------------------------------
# 1. Read raw CSV
# ---------------------------------------------------------------------------
print(f"[products] Reading raw data from {RAW_PATH}")
raw_df = (
    spark.read
    .option("header", "true")
    .schema(PRODUCTS_SCHEMA)
    .csv(RAW_PATH)
)
print(f"[products] Raw row count: {raw_df.count()}")

# ---------------------------------------------------------------------------
# 2. Validate
# ---------------------------------------------------------------------------
rejected = None

valid_df, rejected = drop_null_pk(raw_df, "product_id", rejected, "null product_id")
valid_df, rejected = drop_null_cols(valid_df, ["product_name"], rejected)

# ---------------------------------------------------------------------------
# 3. Deduplicate on product_id
# ---------------------------------------------------------------------------
valid_df = deduplicate(valid_df, pk_col="product_id")
print(f"[products] Valid row count after dedup: {valid_df.count()}")

# ---------------------------------------------------------------------------
# 4. Add ingestion metadata
# ---------------------------------------------------------------------------
valid_df = valid_df.withColumn("ingested_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# 5. Upsert into Delta table (partitioned by department)
# ---------------------------------------------------------------------------
print(f"[products] Upserting into Delta table at {DWH_PATH}")
from common.utils import upsert_to_delta_partitioned
upsert_to_delta_partitioned(
    spark=spark,
    source_df=valid_df,
    delta_path=DWH_PATH,
    merge_key="product_id",
    partition_col="department",
)
print("[products] Upsert complete")

# ---------------------------------------------------------------------------
# 6. Write rejected records
# ---------------------------------------------------------------------------
write_rejected(rejected, REJECTED_PATH, "products")

# ---------------------------------------------------------------------------
# 7. Archive source files
# ---------------------------------------------------------------------------
s3_client = __import__("boto3").client("s3")
paginator = s3_client.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=args["RAW_PREFIX"])
for page in pages:
    for obj in page.get("Contents", []):
        archive_s3_object(BUCKET, obj["Key"], ARCHIVED_PREFIX)

job.commit()
print("[products] Job complete")
