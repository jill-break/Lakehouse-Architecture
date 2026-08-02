"""
Referential integrity via anti/semi joins (HI-2).

The previous implementation collected every distinct order_id in the warehouse
into a Python list on the driver and embedded it in the query plan as a literal
IN (...). These tests pin the distributed behaviour and the edge cases the
collect-based version never handled.
"""

from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common.utils import reject_orphans

ITEMS = StructType(
    [
        StructField("id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("note", StringType(), True),
    ]
)

KEYS = StructType([StructField("order_id", LongType(), True)])


def items(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=ITEMS)


def keys(spark, values):
    return spark.createDataFrame([Row(order_id=v) for v in values], schema=KEYS)


def test_orphans_are_rejected_and_valid_rows_kept(spark):
    df = items(
        spark,
        [
            {"id": 1, "order_id": 100, "note": "ok"},
            {"id": 2, "order_id": 999, "note": "orphan"},
            {"id": 3, "order_id": 101, "note": "ok"},
        ],
    )
    valid, rejected = reject_orphans(
        df, keys(spark, [100, 101]), "order_id", None, "orphan order_id"
    )

    assert sorted(r["id"] for r in valid.collect()) == [1, 3]
    assert rejected.count() == 1
    assert rejected.first()["rejection_reason"] == "orphan order_id"


def test_column_order_is_preserved(spark):
    """A join on a column name moves it to the front; downstream merges care."""
    df = items(spark, [{"id": 1, "order_id": 100, "note": "ok"}])
    valid, rejected = reject_orphans(df, keys(spark, [100]), "order_id", None, "orphan")

    assert valid.columns == ["id", "order_id", "note"]
    assert rejected.columns == ["id", "order_id", "note", "rejection_reason"]


def test_empty_reference_table_rejects_everything(spark):
    df = items(spark, [{"id": 1, "order_id": 100, "note": "x"}])
    valid, rejected = reject_orphans(df, keys(spark, []), "order_id", None, "orphan")

    assert valid.count() == 0
    assert rejected.count() == 1


def test_duplicate_reference_keys_do_not_fan_out(spark):
    """A semi join must not multiply rows the way an inner join would."""
    df = items(spark, [{"id": 1, "order_id": 100, "note": "x"}])
    duplicated_keys = keys(spark, [100, 100, 100])

    valid, _ = reject_orphans(df, duplicated_keys, "order_id", None, "orphan")
    assert valid.count() == 1


def test_null_key_is_treated_as_orphan(spark):
    df = items(spark, [{"id": 1, "order_id": None, "note": "x"}])
    valid, rejected = reject_orphans(df, keys(spark, [100]), "order_id", None, "orphan")

    assert valid.count() == 0
    assert rejected.count() == 1


def test_rejected_frame_accumulates_across_checks(spark):
    """Two FK checks in sequence must not drop the first one's rejects."""
    df = items(
        spark,
        [
            {"id": 1, "order_id": 100, "note": "ok"},
            {"id": 2, "order_id": 999, "note": "orphan"},
        ],
    )
    valid, rejected = reject_orphans(
        df, keys(spark, [100, 101]), "order_id", None, "first check"
    )
    valid, rejected = reject_orphans(
        valid, keys(spark, []), "order_id", rejected, "second check"
    )

    assert valid.count() == 0
    assert rejected.count() == 2
    reasons = {r["rejection_reason"] for r in rejected.collect()}
    assert reasons == {"first check", "second check"}
