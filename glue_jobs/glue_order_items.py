"""
Glue ETL Job: Order Items
Reads order_items Excel from S3 raw zone, validates (including referential
integrity check against the orders Delta table), deduplicates, upserts into
a Delta Lake table partitioned by date, and archives the source file.

Job parameters:
  --S3_BUCKET, --RAW_PREFIX, --DWH_PREFIX, --ARCHIVED_PREFIX,
  --REJECTED_PREFIX, --ORDERS_DWH_PREFIX, --JOB_NAME
"""

import io
import sys

import boto3
import pandas as pd
from delta.tables import DeltaTable

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

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
        "ORDERS_DWH_PREFIX",
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
ORDERS_DWH_PATH = f"s3://{BUCKET}/{args['ORDERS_DWH_PREFIX']}"
ARCHIVED_PREFIX = args["ARCHIVED_PREFIX"]
REJECTED_PATH = f"s3://{BUCKET}/{args['REJECTED_PREFIX']}"

# ---------------------------------------------------------------------------
# 1. Read raw Excel files
# ---------------------------------------------------------------------------
print(f"[order_items] Reading raw data from {RAW_PATH}")

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
    print("[order_items] No raw files found — exiting.")
    job.commit()
    sys.exit(0)

dfs = []
for key in raw_keys:
    resp = s3.get_object(
        Bucket=BUCKET, Key=key, ExpectedBucketOwner=args.get("AWS_ACCOUNT_ID", "")
    )
    body = resp["Body"].read()
    pdf = (
        pd.read_excel(io.BytesIO(body), engine="openpyxl")
        if key.endswith(".xlsx")
        else pd.read_csv(io.BytesIO(body))
    )
    dfs.append(spark.createDataFrame(pdf))

raw_df = dfs[0]
for d in dfs[1:]:
    raw_df = raw_df.union(d)

# Cast columns
raw_df = (
    raw_df.withColumn("id", F.col("id").cast(LongType()))
    .withColumn("order_id", F.col("order_id").cast(LongType()))
    .withColumn("user_id", F.col("user_id").cast(LongType()))
    .withColumn(
        "days_since_prior_order", F.col("days_since_prior_order").cast(IntegerType())
    )
    .withColumn("product_id", F.col("product_id").cast(LongType()))
    .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast(IntegerType()))
    .withColumn("reordered", F.col("reordered").cast(IntegerType()))
    .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
    .withColumn("date", F.to_date(F.col("date")))
)
print(f"[order_items] Raw row count: {raw_df.count()}")

# ---------------------------------------------------------------------------
# 2. Validate
# ---------------------------------------------------------------------------
rejected = None

valid_df, rejected = drop_null_pk(raw_df, "id", rejected, "null id (primary key)")
valid_df, rejected = drop_null_cols(valid_df, ["order_id", "user_id"], rejected)

# Null timestamp check
invalid_ts_mask = F.col("order_timestamp").isNull()
ts_rejected = valid_df.filter(invalid_ts_mask).withColumn(
    "rejection_reason", F.lit("null or unparseable order_timestamp")
)
valid_df = valid_df.filter(~invalid_ts_mask)
rejected = rejected.union(ts_rejected) if rejected is not None else ts_rejected

# ---------------------------------------------------------------------------
# 3. Referential integrity: order_id must exist in orders Delta table
# ---------------------------------------------------------------------------
if DeltaTable.isDeltaTable(spark, ORDERS_DWH_PATH):
    orders_ids = (
        spark.read.format("delta").load(ORDERS_DWH_PATH).select("order_id").distinct()
    )
    orphan_mask = ~F.col("order_id").isin([r.order_id for r in orders_ids.collect()])
    orphan_rejected = valid_df.filter(orphan_mask).withColumn(
        "rejection_reason",
        F.lit("order_id not found in orders table (referential integrity)"),
    )
    valid_df = valid_df.filter(~orphan_mask)
    rejected = (
        rejected.union(orphan_rejected) if rejected is not None else orphan_rejected
    )
    print(
        f"[order_items] Referential integrity check complete."
        f" Orphan rows: {orphan_rejected.count()}"
    )
else:
    print(
        "[order_items] WARNING: orders Delta table not found"
        " — skipping referential integrity check"
    )

# ---------------------------------------------------------------------------
# 4. Deduplicate on id, keep latest by order_timestamp
# ---------------------------------------------------------------------------
valid_df = deduplicate(valid_df, pk_col="id", order_col="order_timestamp")
print(f"[order_items] Valid row count after dedup: {valid_df.count()}")

# ---------------------------------------------------------------------------
# 5. Add ingestion metadata
# ---------------------------------------------------------------------------
valid_df = valid_df.withColumn("ingested_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# 6. Upsert into Delta table partitioned by date
# ---------------------------------------------------------------------------
print(f"[order_items] Upserting into Delta table at {DWH_PATH}")
upsert_to_delta_partitioned(
    spark=spark,
    source_df=valid_df,
    delta_path=DWH_PATH,
    merge_key="id",
    partition_col="date",
)
print("[order_items] Upsert complete")

# ---------------------------------------------------------------------------
# 7. Write rejected records
# ---------------------------------------------------------------------------
write_rejected(rejected, REJECTED_PATH, "order_items")

# ---------------------------------------------------------------------------
# 8. Archive source files
# ---------------------------------------------------------------------------
for key in raw_keys:
    archive_s3_object(BUCKET, key, ARCHIVED_PREFIX)

job.commit()
print("[order_items] Job complete")
