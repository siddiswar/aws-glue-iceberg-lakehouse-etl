from __future__ import annotations

from decimal import Decimal

from pyspark.sql.types import DecimalType

from src.lakehouse_etl.transforms.fact_orders import transform_fact_orders


def test_transform_fact_orders_dedup_and_amount(spark):
    df = spark.createDataFrame(
        [
            # order_id duplicates (same id appears twice)
            ("O1", 2, 3.50),
            ("O1", 2, 3.50),
            ("O2", 3, 10.00),
        ],
        ["order_id", "quantity", "unit_price"],
    )

    out = transform_fact_orders(df)

    # Dedup by order_id
    assert out.select("order_id").distinct().count() == 2
    assert out.count() == 2

    # Check order_amount values (DECIMAL(18,2))
    rows = {r["order_id"]: r["order_amount"] for r in out.select("order_id", "order_amount").collect()}
    assert rows["O1"] == Decimal("7.00")
    assert rows["O2"] == Decimal("30.00")

    # Ensure type is decimal(18,2)
    assert isinstance(out.schema["order_amount"].dataType, DecimalType)
    assert out.schema["order_amount"].dataType.precision == 18
    assert out.schema["order_amount"].dataType.scale == 2


def test_transform_fact_orders_rounding(spark):
    df = spark.createDataFrame(
        [
            ("O3", 3, 0.3333),  # 3 * 0.3333 = 0.9999 -> rounds to 1.00
        ],
        ["order_id", "quantity", "unit_price"],
    )

    out = transform_fact_orders(df)
    r = out.select("order_amount").collect()[0]["order_amount"]

    assert r == Decimal("1.00")
