"""
Upload local data files to the S3 raw zone.

Usage:
    python scripts/upload_raw_data.py --bucket my-lakehouse-bucket [--region us-east-1]
"""

import argparse
import os
import boto3
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent / "Data"

UPLOAD_MAP = {
    "products.csv":             "raw/products/products.csv",
    "orders_apr_2025.xlsx":     "raw/orders/orders_apr_2025.xlsx",
    "order_items_apr_2025.xlsx": "raw/order_items/order_items_apr_2025.xlsx",
}


def upload(bucket: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    for filename, s3_key in UPLOAD_MAP.items():
        local_path = DATA_DIR / filename
        if not local_path.exists():
            print(f"SKIP  {filename} — not found at {local_path}")
            continue

        print(f"Uploading {local_path} -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(local_path), bucket, s3_key)
        print(f"  OK")

    print("\nAll files uploaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw data to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    upload(args.bucket, args.region)
