"""
Generate intentionally bad synthetic data and upload to S3 raw zone.
Used to test the Glue validation and rejection logic.

Bad records introduced:
  products    — 5 rows with null product_id (PK violation)
  orders      — 5 rows with null order_id (PK), 3 rows with null user_id,
                2 rows with invalid timestamp
  order_items — 5 rows with null id (PK), 3 rows with order_id=999999999
                (referential integrity violation — order does not exist)

Usage:
    python scripts/upload_bad_data.py --bucket my-lakehouse-bucket [--region us-east-1]
"""

import argparse
import io
import random

import boto3
import pandas as pd

READY_KEY = "raw/_READY"
DEPARTMENTS = ["Books", "Clothing", "Electronics", "Home", "Sports", "Toys", "Beauty"]
NUM_PRODUCTS = 100
NUM_ORDERS = 50
NUM_USERS = 20


def make_products() -> pd.DataFrame:
    rows = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        dept_id = (product_id % len(DEPARTMENTS)) + 1
        rows.append(
            {
                "product_id": product_id,
                "department_id": dept_id,
                "department": DEPARTMENTS[dept_id - 1],
                "product_name": f"Product_{product_id}",
            }
        )
    df = pd.DataFrame(rows)

    # Inject: 5 rows with null product_id
    bad = df.head(5).copy()
    bad["product_id"] = None
    return pd.concat([df, bad], ignore_index=True)


def make_orders() -> pd.DataFrame:
    rows = []
    base_order_id = 20000
    for i in range(NUM_ORDERS):
        ts = pd.Timestamp("2025-05-01") + pd.Timedelta(hours=random.randint(0, 23))
        rows.append(
            {
                "order_num": i + 1,
                "order_id": base_order_id + i,
                "user_id": random.randint(1, NUM_USERS),
                "order_timestamp": ts.isoformat(),
                "total_amount": round(random.uniform(10.0, 300.0), 2),
                "date": "2025-05-01",
            }
        )
    df = pd.DataFrame(rows)

    # Inject: 5 rows with null order_id
    bad_pk = df.head(5).copy()
    bad_pk["order_id"] = None

    # Inject: 3 rows with null user_id
    bad_user = df.iloc[5:8].copy()
    bad_user["user_id"] = None

    # Inject: 2 rows with invalid timestamp
    bad_ts = df.iloc[8:10].copy()
    bad_ts["order_timestamp"] = "not-a-valid-date"

    return pd.concat([df, bad_pk, bad_user, bad_ts], ignore_index=True)


def make_order_items(orders_df: pd.DataFrame) -> pd.DataFrame:
    valid_orders = orders_df[orders_df["order_id"].notna()].head(NUM_ORDERS)
    rows = []
    item_id = 1
    for order_row in valid_orders.itertuples():
        for cart_pos in range(1, random.randint(2, 5)):
            rows.append(
                {
                    "id": item_id,
                    "order_id": order_row.order_id,
                    "user_id": order_row.user_id,
                    "days_since_prior_order": random.randint(1, 30),
                    "product_id": random.randint(1, NUM_PRODUCTS),
                    "add_to_cart_order": cart_pos,
                    "reordered": random.randint(0, 1),
                    "order_timestamp": order_row.order_timestamp,
                    "date": order_row.date,
                }
            )
            item_id += 1
    df = pd.DataFrame(rows)

    # Inject: 5 rows with null id (PK)
    bad_pk = df.head(5).copy()
    bad_pk["id"] = None

    # Inject: 3 rows with non-existent order_id (referential integrity failure)
    bad_ref = df.iloc[5:8].copy()
    bad_ref["order_id"] = 999999999

    return pd.concat([df, bad_pk, bad_ref], ignore_index=True)


def upload(bucket: str, region: str) -> None:
    random.seed(99)
    s3 = boto3.client("s3", region_name=region)

    products_df = make_products()
    orders_df = make_orders()
    order_items_df = make_order_items(orders_df)

    print(f"Generated {len(products_df)} product rows (including bad)")
    print(f"Generated {len(orders_df)} order rows (including bad)")
    print(f"Generated {len(order_items_df)} order item rows (including bad)")

    # Upload products as CSV
    csv_buf = io.StringIO()
    products_df.to_csv(csv_buf, index=False)
    s3_key = "raw/products/products_bad.csv"
    print(f"\nUploading -> s3://{bucket}/{s3_key}")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buf.getvalue().encode())
    print("  OK")

    # Upload orders and order_items as Excel
    for df, name, prefix in [
        (orders_df, "orders_bad.xlsx", "raw/orders/"),
        (order_items_df, "order_items_bad.xlsx", "raw/order_items/"),
    ]:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        s3_key = f"{prefix}{name}"
        print(f"Uploading -> s3://{bucket}/{s3_key}")
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buf.getvalue())
        print("  OK")

    # Drop the _READY marker
    print(f"\nSignalling pipeline: uploading s3://{bucket}/{READY_KEY}")
    s3.put_object(Bucket=bucket, Key=READY_KEY, Body=b"")
    print("  OK - pipeline will start automatically.")
    print(f"\nExpect rejected records in s3://{bucket}/rejected/")

    print("\nBad records injected:")
    print("  products:    5 null product_id (PK violation)")
    print("  orders:      5 null order_id (PK), 3 null user_id, 2 invalid timestamps")
    print("  order_items: 5 null id (PK), 3 orphan order_id=999999999 (ref integrity)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and upload bad data to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    upload(args.bucket, args.region)
