"""Unit tests for products ETL validation and deduplication logic."""

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from common.utils import deduplicate, drop_null_cols, drop_null_pk

SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True),
    ]
)


def make_df(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=SCHEMA)


def test_drop_null_pk_filters_null_rows(spark):
    df = make_df(
        spark,
        [
            {
                "product_id": 1,
                "department_id": 1,
                "department": "Books",
                "product_name": "A",
            },
            {
                "product_id": None,
                "department_id": 2,
                "department": "Sports",
                "product_name": "B",
            },
        ],
    )
    valid, rejected = drop_null_pk(df, "product_id", None, "null product_id")
    assert valid.count() == 1
    assert rejected.count() == 1
    assert rejected.first()["rejection_reason"] == "null product_id"


def test_drop_null_cols_filters_null_name(spark):
    df = make_df(
        spark,
        [
            {
                "product_id": 1,
                "department_id": 1,
                "department": "Books",
                "product_name": "A",
            },
            {
                "product_id": 2,
                "department_id": 2,
                "department": "Sports",
                "product_name": None,
            },
        ],
    )
    valid, rejected = drop_null_pk(df, "product_id", None, "null pk")
    valid, rejected = drop_null_cols(valid, ["product_name"], rejected)
    assert valid.count() == 1
    assert rejected.count() == 1


def test_deduplicate_keeps_one_row_per_pk(spark):
    df = make_df(
        spark,
        [
            {
                "product_id": 1,
                "department_id": 1,
                "department": "Books",
                "product_name": "A",
            },
            {
                "product_id": 1,
                "department_id": 1,
                "department": "Books",
                "product_name": "A_v2",
            },
            {
                "product_id": 2,
                "department_id": 2,
                "department": "Sports",
                "product_name": "B",
            },
        ],
    )
    result = deduplicate(df, pk_col="product_id")
    assert result.count() == 2


def test_all_valid_rows_pass_through(spark):
    df = make_df(
        spark,
        [
            {
                "product_id": 1,
                "department_id": 1,
                "department": "Books",
                "product_name": "A",
            },
            {
                "product_id": 2,
                "department_id": 2,
                "department": "Sports",
                "product_name": "B",
            },
            {
                "product_id": 3,
                "department_id": 3,
                "department": "Toys",
                "product_name": "C",
            },
        ],
    )
    valid, rejected = drop_null_pk(df, "product_id", None, "null pk")
    valid, rejected = drop_null_cols(valid, ["product_name"], rejected)
    assert valid.count() == 3
    assert rejected is None or rejected.count() == 0
