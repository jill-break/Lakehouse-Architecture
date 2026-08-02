"""
S3 listing and archiving, against moto rather than a real bucket.

These cover the read/archive race (HI-3) and the empty-raw-zone guard (CR-3):
the job lists once, and archives exactly the keys it listed.
"""

import boto3
import pytest
from moto import mock_aws

from common.utils import archive_s3_object, list_raw_keys

BUCKET = "test-lakehouse"


@pytest.fixture
def s3(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def put(client, key, body=b"data"):
    client.put_object(Bucket=BUCKET, Key=key, Body=body)


def test_list_raw_keys_returns_only_data_objects(s3):
    put(s3, "raw/orders/orders.csv")
    put(s3, "raw/orders/orders_2.csv")
    put(s3, "raw/orders/notes.txt")
    put(s3, "raw/orders/", b"")  # folder placeholder
    put(s3, "raw/products/products.csv")  # different prefix

    keys = list_raw_keys(BUCKET, "raw/orders")

    assert sorted(keys) == ["raw/orders/orders.csv", "raw/orders/orders_2.csv"]


def test_list_raw_keys_skips_zero_byte_objects(s3):
    put(s3, "raw/orders/empty.csv", b"")
    assert list_raw_keys(BUCKET, "raw/orders") == []


def test_empty_raw_zone_returns_empty_list(s3):
    """CR-3 regression: the products job used to raise AnalysisException here."""
    assert list_raw_keys(BUCKET, "raw/products") == []


def test_archive_copies_then_deletes(s3):
    put(s3, "raw/orders/orders.csv")

    archive_s3_object(BUCKET, "raw/orders/orders.csv", "archived/orders")

    remaining = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
    assert remaining.get("KeyCount") == 0

    archived = s3.list_objects_v2(Bucket=BUCKET, Prefix="archived/orders/")
    assert archived["KeyCount"] == 1
    assert archived["Contents"][0]["Key"].endswith("_orders.csv")


def test_archive_skips_folder_placeholders(s3):
    put(s3, "raw/orders/", b"")
    archive_s3_object(BUCKET, "raw/orders/", "archived/orders")

    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="archived/").get("KeyCount", 0) == 0


def test_empty_raw_zone_skips_the_job_instead_of_crashing(s3, spark):
    """
    CR-3 regression, end to end.

    Products archives its own source files, so the second run of an unchanged
    pipeline finds an empty prefix. It used to raise AnalysisException, catch
    to JobFailed and page the on-call — for a re-run the docs described as
    exiting cleanly.
    """
    from common.etl import run_dataset_etl
    from glue_products import build_config

    config = build_config(
        {
            "S3_BUCKET": BUCKET,
            "RAW_PREFIX": "raw/products",
            "DWH_PREFIX": "dwh/products",
            "ARCHIVED_PREFIX": "archived/products",
            "REJECTED_PREFIX": "rejected/products",
        }
    )

    summary = run_dataset_etl(spark, config)

    assert summary["status"] == "skipped"
    assert summary["valid_rows"] == 0


def test_a_file_landing_after_the_listing_is_not_archived(s3):
    """
    HI-3 regression.

    The products job used to read with a prefix glob and then archive whatever
    a *later* list call returned, so a file uploaded in between was deleted
    from raw without ever being ingested. Jobs now archive the listed keys.
    """
    put(s3, "raw/products/batch_1.csv")
    listed = list_raw_keys(BUCKET, "raw/products")

    # ... a second upload lands while the job is still running
    put(s3, "raw/products/batch_2.csv")

    for key in listed:
        archive_s3_object(BUCKET, key, "archived/products")

    survivors = [
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/products").get(
            "Contents", []
        )
    ]
    assert survivors == ["raw/products/batch_2.csv"]
