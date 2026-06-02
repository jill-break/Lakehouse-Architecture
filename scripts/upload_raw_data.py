"""
Generate clean synthetic e-commerce data and upload to S3 raw zone.
Drops a _READY marker at the end to trigger the EventBridge → Step Functions pipeline.

Usage:
    python scripts/upload_raw_data.py --bucket my-lakehouse-bucket [--region us-east-1]
"""

import argparse
import io
import random

import boto3
import pandas as pd

READY_KEY = "raw/_READY"

DEPARTMENTS = ["Books", "Clothing", "Electronics", "Home", "Sports", "Toys", "Beauty"]

NUM_PRODUCTS = 1000
NUM_ORDERS = 500
NUM_USERS = 200


def make_products() -> pd.DataFrame:
    rows = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        dept_id = (product_id % len(DEPARTMENTS)) + 1
        dept = DEPARTMENTS[dept_id - 1]
        rows.append(
            {
                "product_id": product_id,
                "department_id": dept_id,
                "department": dept,
                "product_name": f"Product_{product_id}_{dept}",
            }
        )
    return pd.DataFrame(rows)


def make_orders() -> pd.DataFrame:
    rows = []
    base_order_id = 10000
    for i in range(NUM_ORDERS):
        order_id = base_order_id + i
        user_id = random.randint(1, NUM_USERS)
        ts = pd.Timestamp("2025-04-01") + pd.Timedelta(
            hours=random.randint(0, 23), minutes=random.randint(0, 59)
        )
        rows.append(
            {
                "order_num": i + 1,
                "order_id": order_id,
                "user_id": user_id,
                "order_timestamp": ts.isoformat(),
                "total_amount": round(random.uniform(10.0, 500.0), 2),
                "date": "2025-04-01",
            }
        )
    return pd.DataFrame(rows)


def make_order_items(
    orders_df: pd.DataFrame, products_df: pd.DataFrame
) -> pd.DataFrame:
    rows = []
    item_id = 1
    product_ids = products_df["product_id"].tolist()

    for order_row in orders_df.itertuples():
        num_items = random.randint(1, 8)
        selected = random.sample(product_ids, min(num_items, len(product_ids)))
        for cart_pos, product_id in enumerate(selected, start=1):
            rows.append(
                {
                    "id": item_id,
                    "order_id": order_row.order_id,
                    "user_id": order_row.user_id,
                    "days_since_prior_order": random.randint(1, 30),
                    "product_id": product_id,
                    "add_to_cart_order": cart_pos,
                    "reordered": random.randint(0, 1),
                    "order_timestamp": order_row.order_timestamp,
                    "date": order_row.date,
                }
            )
            item_id += 1
    return pd.DataFrame(rows)


def upload(bucket: str, region: str) -> None:
    random.seed(42)
    s3 = boto3.client("s3", region_name=region)

    products_df = make_products()
    orders_df = make_orders()
    order_items_df = make_order_items(orders_df, products_df)

    print(
        f"Generated {len(products_df)} products, {len(orders_df)} orders, {len(order_items_df)} order items"
    )

    # Upload products as CSV
    csv_buf = io.StringIO()
    products_df.to_csv(csv_buf, index=False)
    s3_key = "raw/products/products.csv"
    print(f"Uploading -> s3://{bucket}/{s3_key}")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buf.getvalue().encode())
    print("  OK")

    # Upload orders as Excel
    for df, name, prefix in [
        (orders_df, "orders_apr_2025.xlsx", "raw/orders/"),
        (order_items_df, "order_items_apr_2025.xlsx", "raw/order_items/"),
    ]:
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        s3_key = f"{prefix}{name}"
        print(f"Uploading -> s3://{bucket}/{s3_key}")
        s3.put_object(Bucket=bucket, Key=s3_key, Body=buf.getvalue())
        print("  OK")

    # Drop the _READY marker to trigger the pipeline
    print(f"\nSignalling pipeline: uploading s3://{bucket}/{READY_KEY}")
    s3.put_object(Bucket=bucket, Key=READY_KEY, Body=b"")
    print("  OK - pipeline will start automatically.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and upload clean data to S3")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()
    upload(args.bucket, args.region)
