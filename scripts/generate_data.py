"""
Generate synthetic e-commerce data and write to /tmp/ for CI upload.

Usage:
    python scripts/generate_data.py --type good
    python scripts/generate_data.py --type bad
"""

import argparse
import random
from typing import Dict

import pandas as pd

DEPARTMENTS = ["Books", "Clothing", "Electronics", "Home", "Sports", "Toys", "Beauty"]


def make_products(n: int = 1000) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        dept_id = (i % len(DEPARTMENTS)) + 1
        rows.append(
            {
                "product_id": i,
                "department_id": dept_id,
                "department": DEPARTMENTS[dept_id - 1],
                "product_name": f"Product_{i}_{DEPARTMENTS[dept_id - 1]}",
            }
        )
    return pd.DataFrame(rows)


def make_orders(
    n: int = 500, base_order_id: int = 10000, date: str = "2025-04-01"
) -> pd.DataFrame:
    rows = []
    for i in range(n):
        ts = pd.Timestamp(date) + pd.Timedelta(
            hours=random.randint(0, 23), minutes=random.randint(0, 59)
        )
        rows.append(
            {
                "order_num": i + 1,
                "order_id": base_order_id + i,
                "user_id": random.randint(1, 200),
                "order_timestamp": ts.isoformat(),
                "total_amount": round(random.uniform(10.0, 500.0), 2),
                "date": date,
            }
        )
    return pd.DataFrame(rows)


def make_order_items(orders_df: pd.DataFrame, product_ids: list) -> pd.DataFrame:
    rows = []
    item_id = 1
    for order in orders_df.itertuples():
        num_items = random.randint(1, 8)
        selected = random.sample(product_ids, min(num_items, len(product_ids)))
        for cart_pos, product_id in enumerate(selected, start=1):
            rows.append(
                {
                    "id": item_id,
                    "order_id": order.order_id,
                    "user_id": order.user_id,
                    "days_since_prior_order": random.randint(1, 30),
                    "product_id": product_id,
                    "add_to_cart_order": cart_pos,
                    "reordered": random.randint(0, 1),
                    "order_timestamp": order.order_timestamp,
                    "date": order.date,
                }
            )
            item_id += 1
    return pd.DataFrame(rows)


def generate_good() -> tuple:
    random.seed(42)
    products_df = make_products(n=1000)
    orders_df = make_orders(n=500, base_order_id=10000, date="2025-04-01")
    order_items_df = make_order_items(orders_df, products_df["product_id"].tolist())

    print(
        f"Good data — {len(products_df)} products, {len(orders_df)} orders, "
        f"{len(order_items_df)} order items"
    )
    return products_df, orders_df, order_items_df


def generate_bad() -> tuple:
    random.seed(99)

    # Products — inject 5 null PKs
    products_df = make_products(n=100)
    bad_p = products_df.head(5).copy()
    bad_p["product_id"] = None
    products_df = pd.concat([products_df, bad_p], ignore_index=True)

    # Orders — inject null PKs, null user_id, invalid timestamp
    orders_df = make_orders(n=50, base_order_id=20000, date="2025-05-01")
    bad_oid = orders_df.head(5).copy()
    bad_oid["order_id"] = None
    bad_uid = orders_df.iloc[5:8].copy()
    bad_uid["user_id"] = None
    bad_ts = orders_df.iloc[8:10].copy()
    bad_ts["order_timestamp"] = "not-a-valid-date"
    orders_df = pd.concat([orders_df, bad_oid, bad_uid, bad_ts], ignore_index=True)

    # Order items — inject null PKs and orphan order_id
    clean_orders = make_orders(n=50, base_order_id=20000, date="2025-05-01")
    order_items_df = make_order_items(clean_orders, list(range(1, 101)))
    bad_iid = order_items_df.head(5).copy()
    bad_iid["id"] = None
    bad_ref = order_items_df.iloc[5:8].copy()
    bad_ref["order_id"] = 999999999
    order_items_df = pd.concat([order_items_df, bad_iid, bad_ref], ignore_index=True)

    print(
        f"Bad data — {len(products_df)} product rows, {len(orders_df)} order rows, "
        f"{len(order_items_df)} order item rows"
    )
    print("  products:    5 null product_id (PK violation)")
    print("  orders:      5 null order_id, 3 null user_id, 2 invalid timestamps")
    print("  order_items: 5 null id (PK), 3 orphan order_id=999999999 (ref integrity)")
    return products_df, orders_df, order_items_df


def generate(kind: str) -> Dict[str, pd.DataFrame]:
    """Return {dataset_name: dataframe} for either the good or the bad batch."""
    products_df, orders_df, order_items_df = (
        generate_good() if kind == "good" else generate_bad()
    )
    return {
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
    }


def write_files(
    frames: Dict[str, pd.DataFrame], out_dir: str = "/tmp"
) -> Dict[str, str]:
    """
    Write every dataset as CSV.

    The raw zone is CSV-only on purpose: Spark reads CSV natively and in
    parallel across executors, whereas xlsx has to be pulled into the driver
    and parsed single-threaded by openpyxl. The brief specifies CSV ingestion
    and this project controls its own input format, so there is no reason to
    make the ETL work around a spreadsheet.
    """
    paths = {}
    for name, frame in frames.items():
        path = f"{out_dir}/{name}.csv"
        frame.to_csv(path, index=False)
        paths[name] = path
        print(f"  wrote {path} ({len(frame)} rows)")
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument(
        "--type",
        required=True,
        choices=["good", "bad"],
        help="Type of data to generate",
    )
    parser.add_argument(
        "--out-dir", default="/tmp", help="Directory to write the CSV files into"
    )
    args = parser.parse_args()

    write_files(generate(args.type), args.out_dir)
