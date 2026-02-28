from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType


def build_daily_product_sales(fact_orders: DataFrame, dim_product_current: DataFrame) -> DataFrame:
    enriched = fact_orders.join(
        dim_product_current.select("product_id", "category"),
        on="product_id",
        how="left",
    )
    agg = (
        enriched.groupBy("dt", "product_id", "category")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.sum("quantity").cast("bigint").alias("units_sold"),
            F.round(F.sum("order_amount"), 2).cast(DecimalType(18, 2)).alias("gross_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return agg
