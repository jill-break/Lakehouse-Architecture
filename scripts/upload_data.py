"""
Upload a synthetic batch to the S3 raw zone and trigger the pipeline.

Replaces the two near-identical upload_raw_data.py / upload_bad_data.py
scripts, each of which carried its own copy of the generators that already
live in generate_data.py.

The _READY marker goes up last: EventBridge watches only for that key, so the
pipeline starts exactly once, after every data file is in place.

Usage:
    python scripts/upload_data.py --bucket my-lakehouse-bucket --type good
    python scripts/upload_data.py --bucket my-lakehouse-bucket --type bad
"""

import argparse
import io

import boto3

from generate_data import generate

READY_KEY = "raw/_READY"

# dataset -> (raw prefix, filename stem)
DESTINATIONS = {
    "products": "raw/products",
    "orders": "raw/orders",
    "order_items": "raw/order_items",
}


def upload(bucket: str, region: str, kind: str) -> None:
    s3 = boto3.client("s3", region_name=region)
    frames = generate(kind)
    suffix = "" if kind == "good" else "_bad"

    for name, frame in frames.items():
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        key = f"{DESTINATIONS[name]}/{name}{suffix}.csv"
        print(f"Uploading {len(frame)} rows -> s3://{bucket}/{key}")
        s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().encode())

    print(f"\nSignalling pipeline: s3://{bucket}/{READY_KEY}")
    s3.put_object(Bucket=bucket, Key=READY_KEY, Body=b"")
    print("  OK — the pipeline will start automatically.")

    if kind == "bad":
        print(f"\nExpect rejected records under s3://{bucket}/rejected/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload synthetic data to the raw zone"
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", default="eu-west-1", help="AWS region")
    parser.add_argument("--type", default="good", choices=["good", "bad"])
    args = parser.parse_args()

    upload(args.bucket, args.region, args.type)
