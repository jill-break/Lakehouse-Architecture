"""
The order_items foreign-key hooks.

order_id was checked against the orders table; product_id was documented as a
foreign key in the README and never enforced, so an item referencing a
nonexistent product flowed into the warehouse and silently vanished from the
revenue-by-department join.
"""

from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from glue_order_items import build_config, build_validations, make_fk_hook

ITEMS = StructType(
    [
        StructField("id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("note", StringType(), True),
    ]
)


def items(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows], schema=ITEMS)


def write_reference(spark, path, column, values):
    rows = [Row(**{column: value}) for value in values]
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").save(path)


def test_missing_reference_table_skips_the_check(spark, tmp_path):
    """The first ever run has no orders table — warn, do not crash."""
    hook = make_fk_hook(str(tmp_path / "nothing_here"), "order_id", "orders")
    df = items(spark, [{"id": 1, "order_id": 1, "product_id": 1, "note": "x"}])

    valid, rejected = hook(spark, df, None)

    assert valid.count() == 1
    assert rejected is None


def test_orphan_order_id_is_rejected(spark, tmp_path):
    path = str(tmp_path / "orders")
    write_reference(spark, path, "order_id", [100, 101])
    hook = make_fk_hook(path, "order_id", "orders")

    df = items(
        spark,
        [
            {"id": 1, "order_id": 100, "product_id": 5, "note": "ok"},
            {"id": 2, "order_id": 999, "product_id": 5, "note": "orphan"},
        ],
    )
    valid, rejected = hook(spark, df, None)

    assert valid.count() == 1
    assert rejected.count() == 1
    assert "orders" in rejected.first()["rejection_reason"]


def test_orphan_product_id_is_rejected(spark, tmp_path):
    """The FK the README promised and the code never enforced."""
    path = str(tmp_path / "products")
    write_reference(spark, path, "product_id", [5, 6])
    hook = make_fk_hook(path, "product_id", "products")

    df = items(
        spark,
        [
            {"id": 1, "order_id": 100, "product_id": 5, "note": "ok"},
            {"id": 2, "order_id": 100, "product_id": 4242, "note": "orphan"},
        ],
    )
    valid, rejected = hook(spark, df, None)

    assert valid.count() == 1
    assert rejected.count() == 1
    assert "product_id" in rejected.first()["rejection_reason"]


def test_both_hooks_run_in_sequence(spark, tmp_path):
    orders_path = str(tmp_path / "orders")
    products_path = str(tmp_path / "products")
    write_reference(spark, orders_path, "order_id", [100])
    write_reference(spark, products_path, "product_id", [5])

    args = {
        "S3_BUCKET": "b",
        "RAW_PREFIX": "raw/order_items",
        "DWH_PREFIX": "dwh/order_items",
        "ARCHIVED_PREFIX": "archived/order_items",
        "REJECTED_PREFIX": "rejected/order_items",
        "ORDERS_DWH_PREFIX": "dwh/orders",
        "PRODUCTS_DWH_PREFIX": "dwh/products",
    }
    hooks = [
        make_fk_hook(orders_path, "order_id", "orders"),
        make_fk_hook(products_path, "product_id", "products"),
    ]

    df = items(
        spark,
        [
            {"id": 1, "order_id": 100, "product_id": 5, "note": "ok"},
            {"id": 2, "order_id": 999, "product_id": 5, "note": "bad order"},
            {"id": 3, "order_id": 100, "product_id": 999, "note": "bad product"},
        ],
    )

    valid, rejected = df, None
    for hook in hooks:
        valid, rejected = hook(spark, valid, rejected)

    assert valid.count() == 1
    assert valid.first()["id"] == 1
    assert rejected.count() == 2

    # The job wires up exactly these two checks.
    assert len(build_validations(args)) == 2
    assert build_config(args).pk_col == "id"


def test_config_partitions_by_date_and_orders_by_timestamp():
    args = {
        "S3_BUCKET": "b",
        "RAW_PREFIX": "raw/order_items",
        "DWH_PREFIX": "dwh/order_items",
        "ARCHIVED_PREFIX": "archived/order_items",
        "REJECTED_PREFIX": "rejected/order_items",
        "ORDERS_DWH_PREFIX": "dwh/orders",
        "PRODUCTS_DWH_PREFIX": "dwh/products",
    }
    config = build_config(args)

    assert config.partition_col == "date"
    assert config.order_col == "order_timestamp"
    assert "order_id" in config.required_cols
