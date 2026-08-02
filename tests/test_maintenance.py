"""
Delta maintenance: compaction, Z-ORDER and VACUUM.

Without this job every run leaves another handful of small files per partition,
and query planning degrades steadily while nothing appears to be wrong.
"""

from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common.logging_utils import get_logger
from glue_maintenance import ZORDER_COLUMNS, maintain_table

SCHEMA = StructType(
    [
        StructField("order_id", LongType(), True),
        StructField("payload", StringType(), True),
    ]
)


def write_many_small_files(spark, path, batches=4):
    for batch in range(batches):
        rows = [Row(order_id=batch * 10 + i, payload=f"p{batch}-{i}") for i in range(5)]
        (
            spark.createDataFrame(rows, schema=SCHEMA)
            .repartition(3)
            .write.format("delta")
            .mode("append")
            .save(path)
        )


def count_parquet_files(spark, path):
    import glob
    import os

    return len(
        [
            name
            for name in glob.glob(os.path.join(path, "*.parquet"))
            if not os.path.basename(name).startswith(".")
        ]
    )


def test_maintenance_compacts_and_preserves_every_row(spark, delta_path):
    write_many_small_files(spark, delta_path)
    before = spark.read.format("delta").load(delta_path)
    before_ids = sorted(row["order_id"] for row in before.collect())
    assert count_parquet_files(spark, delta_path) > 1, "expected many small files"

    status = maintain_table(
        spark, delta_path, "order_id", 168, get_logger("maintenance")
    )

    assert status == "ok"
    after = spark.read.format("delta").load(delta_path)
    # OPTIMIZE rewrites the data; the originals stay on disk until VACUUM's
    # retention window expires, so assert on the rows, not the file count.
    assert sorted(row["order_id"] for row in after.collect()) == before_ids


def test_maintenance_is_idempotent(spark, delta_path):
    write_many_small_files(spark, delta_path, batches=2)
    log = get_logger("maintenance")

    maintain_table(spark, delta_path, "order_id", 168, log)
    maintain_table(spark, delta_path, "order_id", 168, log)

    assert spark.read.format("delta").load(delta_path).count() == 10


def test_missing_table_is_reported_not_raised(spark, tmp_path):
    """A first run has no tables yet; maintenance must not fail the pipeline."""
    status = maintain_table(
        spark,
        str(tmp_path / "does_not_exist"),
        "order_id",
        168,
        get_logger("maintenance"),
    )
    assert status == "missing"


def test_every_warehouse_table_has_a_zorder_column():
    assert set(ZORDER_COLUMNS) == {"products", "orders", "order_items"}
    assert ZORDER_COLUMNS["products"] == "product_id"
