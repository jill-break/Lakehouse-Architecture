"""Unit tests for order_items ETL validation and deduplication logic."""

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    TimestampType,
    DateType,
)
from datetime import date, datetime

from common.utils import deduplicate, drop_null_cols, drop_null_pk

SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("user_id", LongType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", LongType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("date", DateType(), True),
    ]
)

TS = datetime(2025, 4, 1, 11, 27, 0)
D = date(2025, 4, 1)


def row(**kwargs):
    defaults = dict(
        id=1,
        order_id=10000,
        user_id=1990,
        days_since_prior_order=10,
        product_id=988,
        add_to_cart_order=1,
        reordered=0,
        order_timestamp=TS,
        date=D,
    )
    defaults.update(kwargs)
    return Row(**defaults)


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_null_id_rejected(spark):
    df = make_df(spark, [row(id=1), row(id=None)])
    valid, rejected = drop_null_pk(df, "id", None, "null id")
    assert valid.count() == 1
    assert rejected.count() == 1


def test_null_order_id_rejected(spark):
    df = make_df(spark, [row(id=1, order_id=None), row(id=2, order_id=10001)])
    valid, _ = drop_null_pk(df, "id", None, "null pk")
    valid, rejected = drop_null_cols(valid, ["order_id"], _)
    assert valid.count() == 1


def test_dedup_removes_duplicate_items(spark):
    df = make_df(spark, [row(id=1), row(id=1), row(id=2)])
    result = deduplicate(df, pk_col="id", order_col="order_timestamp")
    assert result.count() == 2
