"""
Glue ETL Job: Orders
Reads orders Excel file from S3 raw zone, validates, deduplicates,
upserts into a Delta Lake table partitioned by date, and archives the source.

Job parameters:
  --S3_BUCKET, --RAW_PREFIX, --DWH_PREFIX, --ARCHIVED_PREFIX,
  --REJECTED_PREFIX, --JOB_NAME
"""

import io
import sys

import boto3
import pandas as pd

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType

from common.utils import (
    drop_null_pk,
    drop_null_cols,
    deduplicate,
    upsert_to_delta_partitioned,
    write_rejected,
    archive_s3_object,
)

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
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

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

BUCKET = args["S3_BUCKET"]
RAW_PATH = f"s3://{BUCKET}/{args['RAW_PREFIX']}"
DWH_PATH = f"s3://{BUCKET}/{args['DWH_PREFIX']}"
ARCHIVED_PREFIX = args["ARCHIVED_PREFIX"]
REJECTED_PATH = f"s3://{BUCKET}/{args['REJECTED_PREFIX']}"

# ---------------------------------------------------------------------------
# 1. Read raw Excel via pandas → Spark
#    Glue 4.0 workers have pandas available; xlsx needs openpyxl.
# ---------------------------------------------------------------------------
print(f"[orders] Reading raw data from {RAW_PATH}")

s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=BUCKET, Prefix=args["RAW_PREFIX"])

raw_keys = [
    obj["Key"]
    for page in pages
    for obj in page.get("Contents", [])
    if obj["Key"].endswith((".xlsx", ".csv"))
]

if not raw_keys:
    print("[orders] No raw files found — exiting.")
    job.commit()
    sys.exit(0)

dfs = []
for key in raw_keys:
    resp = s3.get_object(
        Bucket=BUCKET, Key=key, ExpectedBucketOwner=args.get("AWS_ACCOUNT_ID", "")
    )
    body = resp["Body"].read()
    if key.endswith(".xlsx"):
        pdf = pd.read_excel(io.BytesIO(body), engine="openpyxl")
    else:
        pdf = pd.read_csv(io.BytesIO(body))
    dfs.append(spark.createDataFrame(pdf))

raw_df = dfs[0]
for d in dfs[1:]:
    raw_df = raw_df.union(d)

# Cast columns to correct types
raw_df = (
    raw_df.withColumn("order_num", F.col("order_num").cast(IntegerType()))
    .withColumn("order_id", F.col("order_id").cast(LongType()))
    .withColumn("user_id", F.col("user_id").cast(LongType()))
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn("date", F.to_date(F.col("date")))
)
print(f"[orders] Raw row count: {raw_df.count()}")

# ---------------------------------------------------------------------------
# 2. Validate
# ---------------------------------------------------------------------------
rejected = None

valid_df, rejected = drop_null_pk(raw_df, "order_id", rejected, "null order_id")
valid_df, rejected = drop_null_cols(valid_df, ["user_id"], rejected)

# Validate timestamps (filter rows where order_timestamp is null after cast)
invalid_ts_mask = F.col("order_timestamp").isNull()
ts_rejected = valid_df.filter(invalid_ts_mask).withColumn(
    "rejection_reason", F.lit("null or unparseable order_timestamp")
)
valid_df = valid_df.filter(~invalid_ts_mask)
rejected = rejected.union(ts_rejected) if rejected is not None else ts_rejected

# ---------------------------------------------------------------------------
# 3. Deduplicate on order_id, keep latest by order_timestamp
# ---------------------------------------------------------------------------
valid_df = deduplicate(valid_df, pk_col="order_id", order_col="order_timestamp")
print(f"[orders] Valid row count after dedup: {valid_df.count()}")

# ---------------------------------------------------------------------------
# 4. Add ingestion metadata
# ---------------------------------------------------------------------------
valid_df = valid_df.withColumn("ingested_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# 5. Upsert into Delta table partitioned by date
# ---------------------------------------------------------------------------
print(f"[orders] Upserting into Delta table at {DWH_PATH}")
upsert_to_delta_partitioned(
    spark=spark,
    source_df=valid_df,
    delta_path=DWH_PATH,
    merge_key="order_id",
    partition_col="date",
)
print("[orders] Upsert complete")

# ---------------------------------------------------------------------------
# 6. Write rejected records
# ---------------------------------------------------------------------------
write_rejected(rejected, REJECTED_PATH, "orders")

# ---------------------------------------------------------------------------
# 7. Archive source files
# ---------------------------------------------------------------------------
for key in raw_keys:
    archive_s3_object(BUCKET, key, ARCHIVED_PREFIX)

job.commit()
print("[orders] Job complete")
