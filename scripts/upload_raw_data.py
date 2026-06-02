"""
Upload local data files to the S3 raw zone, then drop a _READY marker
to trigger the EventBridge → Step Functions pipeline.

Usage:
    python scripts/upload_raw_data.py --bucket my-lakehouse-bucket [--region us-east-1]
"""

import argparse

import boto3
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "Data"

UPLOAD_MAP = {
    "products.csv": "raw/products/products.csv",
    "orders_apr_2025.xlsx": "raw/orders/orders_apr_2025.xlsx",
    "order_items_apr_2025.xlsx": "raw/order_items/order_items_apr_2025.xlsx",
}

READY_KEY = "raw/_READY"


def upload(bucket: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    # 1. Upload all data files
    uploaded = 0
    for filename, s3_key in UPLOAD_MAP.items():
        local_path = DATA_DIR / filename
        if not local_path.exists():
            print(f"SKIP  {filename} — not found at {local_path}")
            continue
        print(f"Uploading {local_path} -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(local_path), bucket, s3_key)
        print("  OK")
        uploaded += 1

    if uploaded == 0:
        print("No files uploaded — aborting. _READY not sent.")
        return

    # 2. Drop the _READY marker — this is the single trigger for EventBridge
    print(f"\nSignalling pipeline: uploading s3://{bucket}/{READY_KEY}")
    s3.put_object(Bucket=bucket, Key=READY_KEY, Body=b"")
    print("  OK — pipeline will start automatically.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw data to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    upload(args.bucket, args.region)
