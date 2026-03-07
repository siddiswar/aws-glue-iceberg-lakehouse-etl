from __future__ import annotations

from pyspark.context import SparkContext
from awsglue.context import GlueContext


def main() -> None:
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    # Create DBs
    for db in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{db}")

    # Create tables
    ddls = [
        # Bronze
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.customers_stg (
          customer_id string,
          full_name   string,
          email       string,
          city        string,
          updated_at  timestamp,
          dt          date,
          ingest_ts   timestamp
        )
        USING iceberg
        PARTITIONED BY (dt)
        """,
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.products_stg (
          product_id   string,
          product_name string,
          category     string,
          list_price   double,
          updated_at   timestamp,
          dt           date,
          ingest_ts    timestamp
        )
        USING iceberg
        PARTITIONED BY (dt)
        """,
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.orders_stg (
          order_id    string,
          order_ts    timestamp,
          customer_id string,
          product_id  string,
          quantity    int,
          unit_price  double,
          dt          date,
          ingest_ts   timestamp
        )
        USING iceberg
        PARTITIONED BY (dt)
        """,

        # Silver
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.dim_customer_scd2 (
          customer_id        string,
          full_name          string,
          email              string,
          city               string,
          hash_diff          string,
          effective_from_ts  timestamp,
          effective_to_ts    timestamp,
          is_current         boolean,
          ingest_ts          timestamp
        )
        USING iceberg
        PARTITIONED BY (is_current)
        """,
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.dim_product_scd2 (
          product_id         string,
          product_name       string,
          category           string,
          list_price         double,
          hash_diff          string,
          effective_from_ts  timestamp,
          effective_to_ts    timestamp,
          is_current         boolean,
          ingest_ts          timestamp
        )
        USING iceberg
        PARTITIONED BY (is_current)
        """,
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.fact_orders (
          order_id     string,
          order_ts     timestamp,
          customer_id  string,
          product_id   string,
          quantity     int,
          unit_price   double,
          order_amount decimal(18,2),
          dt           date,
          ingest_ts    timestamp
        )
        USING iceberg
        PARTITIONED BY (dt)
        """,

        # Gold
        """
        CREATE TABLE IF NOT EXISTS glue_catalog.gold.daily_product_sales (
          dt               date,
          product_id       string,
          category         string,
          orders_count     bigint,
          units_sold       bigint,
          gross_revenue    decimal(18,2),
          unique_customers bigint,
          ingest_ts        timestamp
        )
        USING iceberg
        PARTITIONED BY (dt)
        """,
    ]

    for ddl in ddls:
        spark.sql(ddl)

    print("[INIT] Databases and tables ensured (IF NOT EXISTS).")


if __name__ == "__main__":
    main()
