"""
Deduplication determinism, rejected-record writing and the rejection-rate
circuit breaker.
"""

from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.utils import (
    DataQualityError,
    check_rejection_threshold,
    deduplicate,
    validate_timestamps,
    write_rejected,
)

SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("payload", StringType(), True),
        StructField("ts", TimestampType(), True),
    ]
)

TS = datetime(2025, 4, 1, 10, 0, 0)


def frame(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=SCHEMA)


# ─── Deduplication ────────────────────────────────────────────────────────────


def test_dedup_keeps_latest_by_order_column(spark):
    df = frame(
        spark,
        [
            {"id": 1, "payload": "old", "ts": TS},
            {"id": 1, "payload": "new", "ts": datetime(2025, 4, 1, 12, 0, 0)},
        ],
    )
    result = deduplicate(df, "id", order_col="ts")

    assert result.count() == 1
    assert result.first()["payload"] == "new"


def test_dedup_is_deterministic_on_identical_timestamps(spark):
    """
    MD-4 regression.

    Two rows sharing a primary key *and* a timestamp used to resolve
    arbitrarily, so consecutive runs over a replayed batch could disagree about
    which one survived. The row-hash tiebreak makes the winner reproducible.
    """
    rows = [
        {"id": 1, "payload": "alpha", "ts": TS},
        {"id": 1, "payload": "beta", "ts": TS},
        {"id": 1, "payload": "gamma", "ts": TS},
    ]

    winners = set()
    for ordering in ([0, 1, 2], [2, 0, 1], [1, 2, 0]):
        df = frame(spark, [rows[i] for i in ordering])
        result = deduplicate(df, "id", order_col="ts")
        assert result.count() == 1
        winners.add(result.first()["payload"])

    assert len(winners) == 1, f"non-deterministic survivor: {winners}"


def test_dedup_without_order_column_is_also_deterministic(spark):
    rows = [{"id": 1, "payload": "a", "ts": TS}, {"id": 1, "payload": "b", "ts": TS}]

    first = deduplicate(frame(spark, rows), "id").first()["payload"]
    second = deduplicate(frame(spark, list(reversed(rows))), "id").first()["payload"]
    assert first == second


def test_dedup_preserves_distinct_keys(spark):
    df = frame(
        spark,
        [
            {"id": 1, "payload": "a", "ts": TS},
            {"id": 2, "payload": "b", "ts": TS},
            {"id": 3, "payload": "c", "ts": TS},
        ],
    )
    assert deduplicate(df, "id", order_col="ts").count() == 3


# ─── Timestamps ───────────────────────────────────────────────────────────────


def test_validate_timestamps_rejects_nulls(spark):
    df = frame(
        spark,
        [{"id": 1, "payload": "a", "ts": TS}, {"id": 2, "payload": "b", "ts": None}],
    )
    valid, rejected = validate_timestamps(df, "ts", None)

    assert valid.count() == 1
    assert rejected.count() == 1
    assert "ts" in rejected.first()["rejection_reason"]


# ─── Rejected records ─────────────────────────────────────────────────────────


def test_write_rejected_returns_zero_for_none(spark, tmp_path):
    assert write_rejected(None, str(tmp_path), "orders") == 0


def test_write_rejected_returns_zero_for_empty_frame(spark, tmp_path):
    empty = frame(spark, []).limit(0)
    assert write_rejected(empty, str(tmp_path), "orders") == 0


def test_write_rejected_persists_rows_and_counts_them(spark, tmp_path):
    df = frame(spark, [{"id": 1, "payload": "bad", "ts": TS}])
    count = write_rejected(df, str(tmp_path), "orders", run_id="jr_123")

    assert count == 1
    written = spark.read.parquet(f"{tmp_path}/orders/*")
    assert written.count() == 1
    assert written.first()["payload"] == "bad"


def test_two_batches_in_the_same_second_do_not_clobber(spark, tmp_path):
    """
    LO-3 regression: the path was timestamped to the second and written with
    mode("overwrite"), so a second batch inside the same second replaced the
    first one's rejects instead of adding to them.
    """
    df = frame(spark, [{"id": 1, "payload": "bad", "ts": TS}])
    write_rejected(df, str(tmp_path), "orders", run_id="run_a")
    write_rejected(df, str(tmp_path), "orders", run_id="run_b")

    assert spark.read.parquet(f"{tmp_path}/orders/*").count() == 2


# ─── Circuit breaker ──────────────────────────────────────────────────────────


def test_threshold_allows_an_acceptable_rejection_rate():
    assert check_rejection_threshold(100, 4, 0.05) == pytest.approx(0.04)


def test_threshold_raises_above_the_ceiling():
    with pytest.raises(DataQualityError, match="exceeds the maximum"):
        check_rejection_threshold(100, 40, 0.05)


def test_threshold_catches_a_fully_malformed_batch():
    """Every row rejected used to mean an empty merge and a green pipeline."""
    with pytest.raises(DataQualityError):
        check_rejection_threshold(500, 500, 0.05)


def test_threshold_is_a_no_op_on_an_empty_batch():
    assert check_rejection_threshold(0, 0, 0.05) == 0.0
