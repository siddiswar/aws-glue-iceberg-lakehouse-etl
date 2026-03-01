from __future__ import annotations

from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.lakehouse_etl.transforms.gold import build_daily_product_sales


def test_build_daily_product_sales_aggregations(spark):
    # Fact orders for same dt with two products
    fact = spark.createDataFrame(
        [
            ("2026-02-20", "O1", "C1", "P1", 2, Decimal("10.00")),
            ("2026-02-20", "O2", "C2", "P1", 1, Decimal("5.00")),
            ("2026-02-20", "O3", "C1", "P2", 3, Decimal("7.50")),
        ],
        ["dt", "order_id", "customer_id", "product_id", "quantity", "order_amount"],
    ).withColumn("dt", F.to_date("dt"))

    dimP = spark.createDataFrame(
        [
            ("P1", "Electronics"),
            ("P2", "Books"),
        ],
        ["product_id", "category"],
    )

    out = build_daily_product_sales(fact, dimP)

    rows = {
        (r["dt"].isoformat(), r["product_id"]): r
        for r in out.select(
            "dt", "product_id", "category", "orders_count", "units_sold", "gross_revenue", "unique_customers"
        ).collect()
    }

    # P1
    r1 = rows[("2026-02-20", "P1")]
    assert r1["category"] == "Electronics"
    assert r1["orders_count"] == 2  # O1, O2
    assert r1["units_sold"] == 3  # 2 + 1
    assert r1["gross_revenue"] == Decimal("15.00")
    assert r1["unique_customers"] == 2  # C1, C2

    # P2
    r2 = rows[("2026-02-20", "P2")]
    assert r2["category"] == "Books"
    assert r2["orders_count"] == 1
    assert r2["units_sold"] == 3
    assert r2["gross_revenue"] == Decimal("7.50")
    assert r2["unique_customers"] == 1

    # ingest_ts should exist
    assert out.filter(F.col("ingest_ts").isNull()).count() == 0


def test_build_daily_product_sales_missing_dim_category_is_null(spark):
    # Product P3 not in dim -> left join => category null
    fact = spark.createDataFrame(
        [
            ("2026-02-20", "O1", "C1", "P3", 1, Decimal("2.00")),
        ],
        ["dt", "order_id", "customer_id", "product_id", "quantity", "order_amount"],
    ).withColumn("dt", F.to_date("dt"))

    dimP = spark.createDataFrame(
        [
            ("P1", "Electronics"),
        ],
        ["product_id", "category"],
    )

    out = build_daily_product_sales(fact, dimP)
    r = out.select("category", "gross_revenue").collect()[0]
    assert r["category"] is None
    assert r["gross_revenue"] == Decimal("2.00")


def test_build_daily_product_sales_gross_revenue_decimal_type(spark):
    fact = spark.createDataFrame(
        [
            ("2026-02-20", "O1", "C1", "P1", 1, Decimal("0.335")),
            ("2026-02-20", "O2", "C2", "P1", 1, Decimal("0.335")),
        ],
        ["dt", "order_id", "customer_id", "product_id", "quantity", "order_amount"],
    ).withColumn("dt", F.to_date("dt"))

    dimP = spark.createDataFrame([("P1", "Cat")], ["product_id", "category"])

    out = build_daily_product_sales(fact, dimP)
    r = out.select("gross_revenue").collect()[0]["gross_revenue"]

    # 0.335 + 0.335 = 0.67 (rounded to 2dp)
    assert r == Decimal("0.67")

    # Ensure decimal(18,2)
    assert isinstance(out.schema["gross_revenue"].dataType, DecimalType)
    assert out.schema["gross_revenue"].dataType.precision == 18
    assert out.schema["gross_revenue"].dataType.scale == 2
