"""Unit tests for orders ETL validation and deduplication logic."""

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)
from datetime import date, datetime

from common.utils import deduplicate, drop_null_cols, drop_null_pk


def make_order(spark, rows):
    schema = StructType(
        [
            StructField("order_num", IntegerType(), True),
            StructField("order_id", LongType(), True),
            StructField("user_id", LongType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("date", DateType(), True),
        ]
    )
    return spark.createDataFrame([Row(**r) for r in rows], schema=schema)


TS1 = datetime(2025, 4, 1, 11, 27, 0)
TS2 = datetime(2025, 4, 1, 12, 0, 0)
D1 = date(2025, 4, 1)


def test_null_order_id_rejected(spark):
    df = make_order(
        spark,
        [
            {
                "order_num": 1,
                "order_id": 10000,
                "user_id": 1,
                "order_timestamp": TS1,
                "total_amount": 99.0,
                "date": D1,
            },
            {
                "order_num": 2,
                "order_id": None,
                "user_id": 2,
                "order_timestamp": TS1,
                "total_amount": 50.0,
                "date": D1,
            },
        ],
    )
    valid, rejected = drop_null_pk(df, "order_id", None, "null order_id")
    assert valid.count() == 1
    assert rejected.count() == 1


def test_null_user_id_rejected(spark):
    df = make_order(
        spark,
        [
            {
                "order_num": 1,
                "order_id": 10000,
                "user_id": None,
                "order_timestamp": TS1,
                "total_amount": 99.0,
                "date": D1,
            },
            {
                "order_num": 2,
                "order_id": 10001,
                "user_id": 2,
                "order_timestamp": TS1,
                "total_amount": 50.0,
                "date": D1,
            },
        ],
    )
    valid, _ = drop_null_pk(df, "order_id", None, "null pk")
    valid, rejected = drop_null_cols(valid, ["user_id"], _)
    assert valid.count() == 1
    assert rejected.count() == 1


def test_dedup_keeps_latest_timestamp(spark):
    df = make_order(
        spark,
        [
            {
                "order_num": 1,
                "order_id": 10000,
                "user_id": 1,
                "order_timestamp": TS1,
                "total_amount": 99.0,
                "date": D1,
            },
            {
                "order_num": 2,
                "order_id": 10000,
                "user_id": 1,
                "order_timestamp": TS2,
                "total_amount": 100.0,
                "date": D1,
            },
        ],
    )
    result = deduplicate(df, pk_col="order_id", order_col="order_timestamp")
    assert result.count() == 1
    assert result.first()["total_amount"] == 100.0
