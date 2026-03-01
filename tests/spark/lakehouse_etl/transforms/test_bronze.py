from __future__ import annotations

from pyspark.sql import functions as F

from src.lakehouse_etl.transforms.bronze import (
    transform_customers,
    transform_products,
    transform_orders,
)


def test_transform_customers_trims_and_normalizes(spark):
    df = spark.createDataFrame(
        [
            (" C0001 ", " Alice  ", " ALICE@EXAMPLE.COM ", " London ", "2026-02-20 10:00:00"),
            ("", "Bob", "bob@example.com", "MK", "2026-02-20 10:00:00"),     # invalid id -> filtered
            (None, "Carol", "carol@example.com", "MK", "2026-02-20 10:00:00"),  # invalid id -> filtered
        ],
        ["customer_id", "full_name", "email", "city", "updated_at"],
    )

    out = transform_customers(df, "2026-02-20")

    rows = (
        out.select("customer_id", "full_name", "email", "city", "dt")
        .orderBy("customer_id")
        .collect()
    )

    assert len(rows) == 1
    r = rows[0]

    assert r["customer_id"] == "C0001"
    assert r["full_name"] == "Alice"
    assert r["email"] == "alice@example.com"
    assert r["city"] == "London"
    assert str(r["dt"]) == "2026-02-20"

    # ingest_ts should exist and be non-null
    assert out.filter(F.col("ingest_ts").isNull()).count() == 0


def test_transform_products_casts_price_and_filters_invalid_ids(spark):
    df = spark.createDataFrame(
        [
            (" P1 ", " Prod 1 ", " Cat ", "12.50", "2026-02-20 10:00:00"),
            ("", "Prod2", "Cat", "9.99", "2026-02-20 10:00:00"),  # invalid id -> filtered
        ],
        ["product_id", "product_name", "category", "list_price", "updated_at"],
    )

    out = transform_products(df, "2026-02-20")

    rows = out.select("product_id", "product_name", "category", "list_price", "dt").collect()

    assert len(rows) == 1
    r = rows[0]

    assert r["product_id"] == "P1"
    assert r["product_name"] == "Prod 1"
    assert r["category"] == "Cat"
    assert isinstance(r["list_price"], float)
    assert abs(r["list_price"] - 12.50) < 1e-9
    assert str(r["dt"]) == "2026-02-20"

    assert out.filter(F.col("ingest_ts").isNull()).count() == 0


def test_transform_orders_parses_ts_casts_types_and_filters_quantity(spark):
    df = spark.createDataFrame(
        [
            (" O1 ", " C1 ", " P1 ", "2", "3.5", "2026-02-20 09:00:00"),
            (" O2 ", " C1 ", " P1 ", "0", "3.5", "2026-02-20 09:00:00"),  # quantity 0 -> filtered
            ("", "C1", "P1", "1", "3.5", "2026-02-20 09:00:00"),          # invalid order_id -> filtered
        ],
        ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts"],
    )

    out = transform_orders(df, "2026-02-20")

    rows = out.select("order_id", "customer_id", "product_id", "quantity", "unit_price", "dt").collect()
    assert len(rows) == 1

    r = rows[0]
    assert r["order_id"] == "O1"
    assert r["customer_id"] == "C1"
    assert r["product_id"] == "P1"
    assert r["quantity"] == 2
    assert abs(r["unit_price"] - 3.5) < 1e-9
    assert str(r["dt"]) == "2026-02-20"

    assert out.filter(F.col("ingest_ts").isNull()).count() == 0