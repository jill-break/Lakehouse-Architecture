"""
Regression tests for the Delta merge — the most business-critical function in
the codebase and, until now, the least covered.

Each test here maps to a defect that silently corrupted the warehouse:
duplicated rows that made every SUM() drift upwards, run after run, while the
pipeline stayed green.
"""

from datetime import date

import pytest
from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.utils import upsert_to_delta

SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("customer", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("date", DateType(), True),
    ]
)

D1 = date(2025, 4, 1)
D2 = date(2025, 4, 2)


def frame(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=SCHEMA)


def read(spark, path):
    return spark.read.format("delta").load(path)


def test_first_write_creates_partitioned_table(spark, delta_path):
    df = frame(
        spark, [{"order_id": 1, "customer": "a", "total_amount": 10.0, "date": D1}]
    )
    upsert_to_delta(spark, df, delta_path, merge_key="order_id", partition_col="date")

    assert read(spark, delta_path).count() == 1
    assert (
        "date"
        in DeltaTable.forPath(spark, delta_path).detail().first()["partitionColumns"]
    )


def test_merge_is_idempotent(spark, delta_path):
    """Re-running the same batch must not change the row count or the values."""
    rows = [
        {"order_id": 1, "customer": "a", "total_amount": 10.0, "date": D1},
        {"order_id": 2, "customer": "b", "total_amount": 20.0, "date": D1},
    ]
    df = frame(spark, rows)

    upsert_to_delta(spark, df, delta_path, "order_id", "date")
    upsert_to_delta(spark, df, delta_path, "order_id", "date")
    upsert_to_delta(spark, df, delta_path, "order_id", "date")

    result = read(spark, delta_path)
    assert result.count() == 2
    assert result.select("order_id").distinct().count() == 2


def test_merge_updates_existing_row_in_place(spark, delta_path):
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "a", "total_amount": 10.0, "date": D1}]
        ),
        delta_path,
        "order_id",
        "date",
    )
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "a", "total_amount": 99.0, "date": D1}]
        ),
        delta_path,
        "order_id",
        "date",
    )

    result = read(spark, delta_path)
    assert result.count() == 1
    assert result.first()["total_amount"] == 99.0


def test_null_partition_value_does_not_duplicate(spark, delta_path):
    """
    CR-1 regression.

    With the partition column in the merge predicate, `NULL = NULL` is UNKNOWN,
    so a row with a null partition never matched its own target row and was
    re-inserted on every single run — unbounded growth, worst on exactly the
    dirty records the pipeline exists to catch.
    """
    df = frame(
        spark, [{"order_id": 7, "customer": "a", "total_amount": 10.0, "date": None}]
    )

    upsert_to_delta(spark, df, delta_path, "order_id", "date")
    upsert_to_delta(spark, df, delta_path, "order_id", "date")
    upsert_to_delta(spark, df, delta_path, "order_id", "date")

    assert read(spark, delta_path).count() == 1


def test_partition_value_change_updates_rather_than_duplicates(spark, delta_path):
    """
    HI-1 regression.

    When a record legitimately moves partition — an order date corrected, a
    product reclassified — the target row lives in a different partition. A
    predicate that includes the partition column cannot match it, so the merge
    inserts a second row carrying the same primary key.
    """
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "a", "total_amount": 10.0, "date": D1}]
        ),
        delta_path,
        "order_id",
        "date",
    )
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "a", "total_amount": 10.0, "date": D2}]
        ),
        delta_path,
        "order_id",
        "date",
    )

    result = read(spark, delta_path)
    assert result.count() == 1, "primary key duplicated across partitions"
    assert result.first()["date"] == D2


def test_unpartitioned_table_merges(spark, delta_path):
    """Products is unpartitioned — the same helper must handle both shapes."""
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "a", "total_amount": 1.0, "date": D1}]
        ),
        delta_path,
        "order_id",
    )
    upsert_to_delta(
        spark,
        frame(
            spark, [{"order_id": 1, "customer": "z", "total_amount": 2.0, "date": D1}]
        ),
        delta_path,
        "order_id",
    )

    result = read(spark, delta_path)
    assert result.count() == 1
    assert result.first()["customer"] == "z"


@pytest.mark.parametrize("batches", [1, 2, 5])
def test_row_count_tracks_distinct_keys_only(spark, delta_path, batches):
    """No matter how many times a batch is replayed, keys stay unique."""
    df = frame(
        spark,
        [
            {"order_id": i, "customer": "x", "total_amount": float(i), "date": D1}
            for i in range(1, 6)
        ],
    )
    for _ in range(batches):
        upsert_to_delta(spark, df, delta_path, "order_id", "date")

    assert read(spark, delta_path).count() == 5
