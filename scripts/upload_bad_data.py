"""
Upload intentionally bad data to S3 raw zone to test validation and rejection logic.

Bad records introduced:
  products    — 5 rows with null product_id (PK violation)
  orders      — 5 rows with null order_id (PK), 3 rows with null user_id,
                2 rows with invalid timestamp
  order_items — 5 rows with null id (PK), 3 rows with an order_id that does
                not exist in orders (referential integrity violation)
"""

import argparse
import io

import boto3
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "Data"
READY_KEY = "raw/_READY"


def make_bad_products() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "products.csv")
    bad = df.head(5).copy()
    bad["product_id"] = None  # null PK — will be rejected
    return pd.concat([df, bad], ignore_index=True)


def make_bad_orders() -> pd.DataFrame:
    df = pd.read_excel(DATA_DIR / "orders_apr_2025.xlsx", engine="openpyxl")
    # Null order_id
    bad_pk = df.head(5).copy()
    bad_pk["order_id"] = None
    # Null user_id
    bad_user = df.iloc[5:8].copy()
    bad_user["user_id"] = None
    # Invalid timestamp
    bad_ts = df.iloc[8:10].copy()
    bad_ts["order_timestamp"] = "not-a-date"
    return pd.concat([df, bad_pk, bad_user, bad_ts], ignore_index=True)


def make_bad_order_items() -> pd.DataFrame:
    df = pd.read_excel(DATA_DIR / "order_items_apr_2025.xlsx", engine="openpyxl")
    # Null id (PK)
    bad_pk = df.head(5).copy()
    bad_pk["id"] = None
    # Orphan order_id (referential integrity failure)
    bad_ref = df.iloc[5:8].copy()
    bad_ref["order_id"] = 999999999  # order that doesn't exist
    return pd.concat([df, bad_pk, bad_ref], ignore_index=True)


def upload(bucket: str, region: str) -> None:
    s3 = boto3.client("s3", region_name=region)

    uploads = {
        "raw/products/products_bad.csv": make_bad_products()
        .to_csv(index=False)
        .encode(),
        "raw/orders/orders_bad.xlsx": _to_xlsx(make_bad_orders()),
        "raw/order_items/order_items_bad.xlsx": _to_xlsx(make_bad_order_items()),
    }

    for s3_key, data in uploads.items():
        print(f"Uploading bad data -> s3://{bucket}/{s3_key}")
        s3.put_object(Bucket=bucket, Key=s3_key, Body=data)
        print("  OK")

    print(f"\nSignalling pipeline: uploading s3://{bucket}/{READY_KEY}")
    s3.put_object(Bucket=bucket, Key=READY_KEY, Body=b"")
    print("  OK - pipeline will start automatically.")
    print("\nExpect rejected records in s3://{bucket}/rejected/")


def _to_xlsx(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload bad data to S3 for rejection testing"
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    upload(args.bucket, args.region)
