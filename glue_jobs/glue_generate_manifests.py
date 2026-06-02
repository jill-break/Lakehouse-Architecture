"""
Glue job: Generate Delta Lake symlink manifests for Athena compatibility.
Run once after each ETL pipeline to keep manifests in sync.
"""

import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from delta.tables import DeltaTable

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

BUCKET = args["S3_BUCKET"]

tables = ["products", "orders", "order_items"]

for table in tables:
    path = f"s3://{BUCKET}/lakehouse-dwh/{table}"
    print(f"Generating manifest for {path}")
    dt = DeltaTable.forPath(spark, path)
    dt.generate("symlink_format_manifest")
    print(f"  Done — {table}")

job.commit()
print("All manifests generated.")
