from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from lakehouse_etl.util.spark import RAW_TS_FORMAT


def transform_customers(df: DataFrame, dt: str) -> DataFrame:
    return (
        df.withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("full_name", F.trim(F.col("full_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at"), RAW_TS_FORMAT))
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
        .withColumn("dt", F.to_date(F.lit(dt)))
        .withColumn("ingest_ts", F.current_timestamp())
    )


def transform_products(df: DataFrame, dt: str) -> DataFrame:
    return (
        df.withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("list_price", F.col("list_price").cast("double"))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at"), RAW_TS_FORMAT))
        .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
        .withColumn("dt", F.to_date(F.lit(dt)))
        .withColumn("ingest_ts", F.current_timestamp())
    )


def transform_orders(df: DataFrame, dt: str) -> DataFrame:
    return (
        df.withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("order_ts", F.to_timestamp(F.col("order_ts"), RAW_TS_FORMAT))
        .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
        .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
        .withColumn("dt", F.to_date(F.lit(dt)))
        .withColumn("ingest_ts", F.current_timestamp())
    )
