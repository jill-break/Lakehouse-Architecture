"""
End-to-end integration: CSV on disk -> read -> validate -> merge -> query the
Delta table, using the same DatasetConfig objects the Glue jobs ship with.

This is the test that would have caught most of the correctness defects at
once: it runs the real reader options, the real validation chain and the real
merge against a real Delta table.
"""

import pytest

from common.etl import read_paths, transform
from common.utils import upsert_to_delta
from glue_orders import build_config as orders_config
from glue_products import build_config as products_config

pytestmark = pytest.mark.integration

ORDERS_HEADER = "order_num,order_id,user_id,order_timestamp,total_amount,date"
GOOD_ORDERS = [
    "1,10001,501,2025-04-01T10:15:00,120.50,2025-04-01",
    "2,10002,502,2025-04-01T11:20:00,80.00,2025-04-01",
    "3,10003,503,2025-04-02T09:05:00,45.25,2025-04-02",
]
DIRTY_ORDERS = [
    ",,504,2025-04-01T12:00:00,10.00,2025-04-01",  # null order_id (PK)
    "5,10005,,2025-04-01T12:30:00,15.00,2025-04-01",  # null user_id
    "6,10006,506,not-a-timestamp,20.00,2025-04-01",  # unparseable timestamp
    "7,10007,507,2025-04-01T13:00:00,25.00,",  # null partition value
]


def write_csv(tmp_path, name, header, rows):
    path = tmp_path / name
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return str(path)


def args_for(tmp_path, dataset):
    return {
        "S3_BUCKET": "unused-in-this-test",
        "RAW_PREFIX": f"raw/{dataset}",
        "DWH_PREFIX": f"dwh/{dataset}",
        "ARCHIVED_PREFIX": f"archived/{dataset}",
        "REJECTED_PREFIX": f"rejected/{dataset}",
        "ORDERS_DWH_PREFIX": "dwh/orders",
        "PRODUCTS_DWH_PREFIX": "dwh/products",
    }


def run(spark, config, csv_paths, delta_path):
    raw = read_paths(spark, config.schema, csv_paths)
    valid, rejected, stats = transform(spark, raw, config)
    upsert_to_delta(spark, valid, delta_path, config.pk_col, config.partition_col)
    return valid, rejected, stats


def test_clean_batch_lands_in_delta(spark, tmp_path):
    config = orders_config(args_for(tmp_path, "orders"))
    csv = write_csv(tmp_path, "orders.csv", ORDERS_HEADER, GOOD_ORDERS)
    delta_path = str(tmp_path / "orders_delta")

    _, _, stats = run(spark, config, [csv], delta_path)

    assert stats["total_rows"] == 3
    assert stats["valid_rows"] == 3
    assert stats["rejected_rows"] == 0

    table = spark.read.format("delta").load(delta_path)
    assert table.count() == 3
    assert set(table.columns) >= {"order_id", "total_amount", "date", "ingested_at"}


def test_dirty_rows_are_rejected_with_reasons(spark, tmp_path):
    config = orders_config(args_for(tmp_path, "orders"))
    csv = write_csv(tmp_path, "orders.csv", ORDERS_HEADER, GOOD_ORDERS + DIRTY_ORDERS)
    delta_path = str(tmp_path / "orders_delta")

    _, rejected, stats = run(spark, config, [csv], delta_path)

    assert stats["valid_rows"] == 3
    assert stats["rejected_rows"] == 4

    reasons = " | ".join(r["rejection_reason"] for r in rejected.collect())
    assert "order_id" in reasons
    assert "user_id" in reasons
    assert "order_timestamp" in reasons
    assert "partition" in reasons

    # None of the bad rows reached the warehouse.
    assert spark.read.format("delta").load(delta_path).count() == 3


def test_two_files_in_one_batch_are_combined(spark, tmp_path):
    config = orders_config(args_for(tmp_path, "orders"))
    first = write_csv(tmp_path, "orders_a.csv", ORDERS_HEADER, GOOD_ORDERS[:2])
    second = write_csv(tmp_path, "orders_b.csv", ORDERS_HEADER, GOOD_ORDERS[2:])
    delta_path = str(tmp_path / "orders_delta")

    _, _, stats = run(spark, config, [first, second], delta_path)

    assert stats["total_rows"] == 3
    assert spark.read.format("delta").load(delta_path).count() == 3


def test_reordered_header_fails_loudly(spark, tmp_path):
    """
    HI-5 / MD-3 regression.

    Two files with the same columns in a different order used to be unioned
    positionally, quietly loading user_id values into order_id — both are
    integers, so nothing complained and the corruption was invisible.

    With enforceSchema=false Spark checks each file's header against the
    declared schema and refuses to read a mismatched one. A failed run that
    names the offending file beats a successful run with swapped values.
    """
    config = orders_config(args_for(tmp_path, "orders"))
    normal = write_csv(tmp_path, "a.csv", ORDERS_HEADER, [GOOD_ORDERS[0]])
    swapped = write_csv(
        tmp_path,
        "b.csv",
        "order_num,user_id,order_id,order_timestamp,total_amount,date",
        ["2,777,10002,2025-04-01T11:20:00,80.00,2025-04-01"],
    )
    delta_path = str(tmp_path / "orders_delta")

    with pytest.raises(Exception) as excinfo:
        run(spark, config, [normal, swapped], delta_path)

    message = str(excinfo.value)
    assert "CSV header does not conform to the schema" in message
    assert "b.csv" in message


def test_renamed_column_fails_loudly(spark, tmp_path):
    """A renamed column is a schema break, not something to silently null out."""
    config = orders_config(args_for(tmp_path, "orders"))
    renamed = write_csv(
        tmp_path,
        "c.csv",
        "order_num,order_id,customer_id,order_timestamp,total_amount,date",
        [GOOD_ORDERS[0]],
    )

    with pytest.raises(Exception, match="CSV header does not conform"):
        run(spark, config, [renamed], str(tmp_path / "orders_delta"))


def test_rerunning_the_whole_batch_is_idempotent(spark, tmp_path):
    config = orders_config(args_for(tmp_path, "orders"))
    csv = write_csv(tmp_path, "orders.csv", ORDERS_HEADER, GOOD_ORDERS + DIRTY_ORDERS)
    delta_path = str(tmp_path / "orders_delta")

    run(spark, config, [csv], delta_path)
    first_pass = spark.read.format("delta").load(delta_path).count()

    run(spark, config, [csv], delta_path)
    run(spark, config, [csv], delta_path)

    table = spark.read.format("delta").load(delta_path)
    assert table.count() == first_pass
    assert table.select("order_id").distinct().count() == first_pass


def test_products_batch_is_unpartitioned_and_deduplicated(spark, tmp_path):
    config = products_config(args_for(tmp_path, "products"))
    csv = write_csv(
        tmp_path,
        "products.csv",
        "product_id,department_id,department,product_name",
        [
            "1,1,Books,Product_1",
            "2,2,Clothing,Product_2",
            "2,2,Clothing,Product_2_duplicate",
            ",3,Toys,Product_orphan",
        ],
    )
    delta_path = str(tmp_path / "products_delta")

    _, rejected, stats = run(spark, config, [csv], delta_path)

    assert stats["rejected_rows"] == 1
    assert stats["valid_rows"] == 2
    assert spark.read.format("delta").load(delta_path).count() == 2
    assert config.partition_col is None


def test_a_product_changing_department_updates_in_place(spark, tmp_path):
    """The reclassification case that a partitioned merge key got wrong."""
    config = products_config(args_for(tmp_path, "products"))
    delta_path = str(tmp_path / "products_delta")
    header = "product_id,department_id,department,product_name"

    run(
        spark,
        config,
        [write_csv(tmp_path, "p1.csv", header, ["1,1,Books,Widget"])],
        delta_path,
    )
    run(
        spark,
        config,
        [write_csv(tmp_path, "p2.csv", header, ["1,3,Toys,Widget"])],
        delta_path,
    )

    table = spark.read.format("delta").load(delta_path)
    assert table.count() == 1
    assert table.first()["department"] == "Toys"
