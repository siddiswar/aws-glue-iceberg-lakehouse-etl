from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- Databases ---
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.silver")
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.gold")


def table_ok(full_name: str) -> bool:
    """
    Returns True if the table exists and its Iceberg metadata can be loaded.
    This catches the common dev failure: catalog entry exists but S3 metadata files were deleted.
    """
    try:
        # DESCRIBE is usually enough to force Iceberg metadata load
        spark.sql(f"DESCRIBE TABLE {full_name}")
        return True
    except Exception as e:
        print(f"[INIT][REPAIR] Table check failed for {full_name}: {e}")
        return False


def drop_table_if_exists(full_name: str):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    except Exception as e:
        # Best effort
        print(f"[INIT][REPAIR] DROP TABLE failed for {full_name}: {e}")


def ensure_table(full_name: str, create_sql: str):
    """
    Create if missing; if exists but broken, drop & recreate.
    """
    if table_ok(full_name):
        print(f"[INIT] OK: {full_name}")
        return

    print(f"[INIT][REPAIR] Recreating table: {full_name}")
    drop_table_if_exists(full_name)
    spark.sql(create_sql)
    print(f"[INIT][REPAIR] Recreated: {full_name}")


# --- CREATE TABLE statements (Iceberg uses the warehouse location from Glue job Spark conf) ---

ensure_table(
    "glue_catalog.bronze.customers_stg",
    """
    CREATE TABLE glue_catalog.bronze.customers_stg (
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
    """
)

ensure_table(
    "glue_catalog.bronze.products_stg",
    """
    CREATE TABLE glue_catalog.bronze.products_stg (
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
    """
)

ensure_table(
    "glue_catalog.bronze.orders_stg",
    """
    CREATE TABLE glue_catalog.bronze.orders_stg (
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
    """
)

ensure_table(
    "glue_catalog.silver.dim_customer_scd2",
    """
    CREATE TABLE glue_catalog.silver.dim_customer_scd2 (
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
    """
)

ensure_table(
    "glue_catalog.silver.dim_product_scd2",
    """
    CREATE TABLE glue_catalog.silver.dim_product_scd2 (
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
    """
)

ensure_table(
    "glue_catalog.silver.fact_orders",
    """
    CREATE TABLE glue_catalog.silver.fact_orders (
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
    """
)

ensure_table(
    "glue_catalog.gold.daily_product_sales",
    """
    CREATE TABLE glue_catalog.gold.daily_product_sales (
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
    """
)

print("[INIT] Completed init + repair checks.")
