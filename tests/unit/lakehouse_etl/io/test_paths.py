from __future__ import annotations

from src.lakehouse_etl.io.paths import (
    raw_entity_path,
    raw_customers_path,
    raw_products_path,
    raw_orders_path,
)


def test_raw_entity_path_builds_expected_s3_uri():
    out = raw_entity_path(
        bucket="my-bucket",
        entity="customers",
        dt="2026-02-20",
        filename="customers.csv",
    )
    assert out == "s3://my-bucket/raw/customers/dt=2026-02-20/customers.csv"


def test_raw_customers_path():
    assert (
            raw_customers_path("b1", "2026-02-20")
            == "s3://b1/raw/customers/dt=2026-02-20/customers.csv"
    )


def test_raw_products_path():
    assert (
            raw_products_path("b1", "2026-02-20")
            == "s3://b1/raw/products/dt=2026-02-20/products.csv"
    )


def test_raw_orders_path():
    assert (
            raw_orders_path("b1", "2026-02-20")
            == "s3://b1/raw/orders/dt=2026-02-20/orders.csv"
    )
