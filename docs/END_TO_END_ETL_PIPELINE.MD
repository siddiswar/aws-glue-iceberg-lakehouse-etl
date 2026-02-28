## 🏗️ End-to-End Lakehouse ETL (Detailed)

This repository implements a complete **Bronze → Silver → Gold** lakehouse pipeline on AWS using:

- **Amazon S3** for storage (raw + Iceberg warehouse)
- **Apache Iceberg** as the table format (metadata + snapshots)
- **Parquet** as the data file format
- **AWS Glue (Spark / PySpark)** for ETL jobs
- **AWS Step Functions** to orchestrate the workflow and enforce ordering
- **AWS Lambda** for control-plane tasks (date selection + mock source generation)
- **Amazon DynamoDB** to maintain state for the mock source producer (idempotency + monotonic IDs)

The pipeline models a common star schema scenario:

- **Dimensions (SCD2):** customers, products
- **Fact (incremental):** orders (append-only per day)
- **Gold aggregate:** daily product sales metrics

---

## 🔁 Orchestration Flow (Step Functions)

A Step Functions state machine orchestrates the pipeline in this sequence:

1) **Compute processing date (`dt`)**
2) **Init/Repair Iceberg tables** (ensures Glue Catalog + Iceberg metadata are consistent)
3) **Mock source data producer** (generates raw CSV snapshots + daily orders)
4) **Bronze ingestion** jobs (raw → Bronze staging)
5) **Silver** jobs (SCD2 merges + fact load)
6) **Gold** job (daily aggregate)

> **Important:** Glue `.sync` tasks return a “job run output” JSON. Each Glue state stores its output under a `ResultPath` so the state machine always retains `dt` for downstream tasks.

---

## 📦 Storage Layout (S3)

### Raw landing zone (CSV)
The mock producer writes one file per entity per day:

- `s3://<bucket>/raw/customers/dt=YYYY-MM-DD/customers.csv`
- `s3://<bucket>/raw/products/dt=YYYY-MM-DD/products.csv`
- `s3://<bucket>/raw/orders/dt=YYYY-MM-DD/orders.csv`

### Iceberg warehouse
Iceberg tables write Parquet data + Iceberg metadata under:

- `s3://<bucket>/warehouse/bronze/...`
- `s3://<bucket>/warehouse/silver/...`
- `s3://<bucket>/warehouse/gold/...`

> S3 folders are logical prefixes. `warehouse/` appears as soon as Iceberg writes objects.

---

## 🧠 Mock Source Data Producer (Lambda)

### Why it exists
For learning and repeatable testing, this pipeline includes a Lambda that acts as an upstream “source system”.
It generates **realistic daily changes** without manual CSV creation.

### State management (DynamoDB)
The producer uses DynamoDB for two purposes:

1) **Idempotency per day**
- Stores a run marker: `pk = run#YYYY-MM-DD`
- Prevents accidental re-generation for the same `dt`

2) **Monotonic IDs**
- Stores atomic counters:
  - `pk = counter#customers`
  - `pk = counter#products`
  - `pk = counter#orders`
- Guarantees IDs continue increasing across days

### Self-healing behavior (important)
If DynamoDB says a day was generated but the S3 files are missing (e.g., you emptied the bucket),
the Lambda will **regenerate** the raw files. This avoids pipeline failure due to missing raw inputs.

### What it generates
**Customers snapshot (daily)**
- Exports full current snapshot each day
- Columns typically include:
  - `customer_id, full_name, email, city, updated_at`

**Products snapshot (daily)**
- Columns typically include:
  - `product_id, product_name, category, list_price, updated_at`

**Orders (incremental daily)**
- Generates N orders for the day referencing existing customers/products
- Columns typically include:
  - `order_id, order_ts, customer_id, product_id, quantity, unit_price`

### Notes about DynamoDB numeric types
DynamoDB does not support Python `float` via boto3 serialization.
The producer converts numeric floats (e.g., `list_price`) into `Decimal` before writing state records.

---

## 🧱 Glue Jobs (Detailed)

All jobs run on **Glue 4.0** and use Iceberg via:
- `--datalake-formats=iceberg`
- Spark conf:
  - `spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog`
  - `spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog`
  - `spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO`
  - `spark.sql.catalog.glue_catalog.warehouse=s3://<bucket>/warehouse/`
  - `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`

---

### 0) `init_iceberg_tables` (self-repairing)
**Goal:** Ensure Glue Catalog databases + Iceberg tables exist and are not “dangling”.

What it does:
- Creates databases if missing:
  - `bronze`, `silver`, `gold`
- Creates Iceberg tables if missing
- **Self-repair:** if a table exists in the catalog but its Iceberg metadata files are missing in S3,
  the job drops and recreates the table. This prevents “S3 404 key does not exist” failures.

Tables created:
- Bronze:
  - `bronze.customers_stg`
  - `bronze.products_stg`
  - `bronze.orders_stg`
- Silver:
  - `silver.dim_customer_scd2`
  - `silver.dim_product_scd2`
  - `silver.fact_orders`
- Gold:
  - `gold.daily_product_sales`

---

## 🥉 Bronze Layer (raw → standardized staging)

### 1) `bronze_ingest_customers`
**Input:** `raw/customers/dt=<dt>/customers.csv`  
**Output:** `bronze.customers_stg` (Iceberg, partitioned by `dt`)

Transform rules:
- trim whitespace on string fields
- normalize email: `lower(trim(email))`
- parse `updated_at` to timestamp
- drop invalid rows (missing `customer_id`)
- add operational columns:
  - `dt = <dt>`
  - `ingest_ts = current_timestamp()`

---

### 2) `bronze_ingest_products`
**Input:** `raw/products/dt=<dt>/products.csv`  
**Output:** `bronze.products_stg` (partitioned by `dt`)

Transform rules:
- trim string fields
- parse `updated_at`
- cast `list_price` to numeric
- drop invalid rows (missing `product_id`)
- add `dt`, `ingest_ts`

---

### 3) `bronze_ingest_orders`
**Input:** `raw/orders/dt=<dt>/orders.csv`  
**Output:** `bronze.orders_stg` (partitioned by `dt`)

Transform rules:
- parse `order_ts` timestamp
- drop invalid rows (missing `order_id`)
- enforce `quantity > 0`
- add `dt`, `ingest_ts`

---

## 🥈 Silver Layer (modeling: SCD2 dims + incremental fact)

### 4) `silver_merge_dim_customer_scd2`
**Input:** `bronze.customers_stg` filtered to `dt=<dt>`  
**Output:** `silver.dim_customer_scd2`

Tracked attributes:
- `full_name, email, city`

SCD2 logic:
- compute `hash_diff` = SHA-256 hash of tracked attributes
- for each `customer_id`:
  - if current row exists and `hash_diff` changed:
    - expire it: `is_current=false`, `effective_to_ts=current_timestamp()`
  - insert new current row:
    - `effective_from_ts=current_timestamp()`
    - `effective_to_ts=NULL`
    - `is_current=true`

---

### 5) `silver_merge_dim_product_scd2`
**Input:** `bronze.products_stg` for `dt=<dt>`  
**Output:** `silver.dim_product_scd2`

Tracked attributes:
- `product_name, category, list_price`

Same SCD2 logic pattern as customers.

---

### 6) `silver_load_fact_orders`
**Input:** `bronze.orders_stg` for `dt=<dt>`  
**Output:** `silver.fact_orders` (partitioned by `dt`)

Fact logic:
- de-duplicate within day by `order_id`
- compute `order_amount = round(quantity * unit_price, 2)` and store as `DECIMAL(18,2)`
- idempotent daily loads:
  - delete existing `dt=<dt>` partition (if present)
  - append the day’s rows

---

## 🥇 Gold Layer (analytics aggregate)

### 7) `gold_build_daily_product_sales`
**Inputs:**
- `silver.fact_orders` for `dt=<dt>`
- current product dimension: `silver.dim_product_scd2` where `is_current=true`

Join:
- `fact.product_id = dim.product_id` (current dimension version)

Aggregation (group by `dt, product_id, category`):
- `orders_count = countDistinct(order_id)`
- `units_sold = sum(quantity)`
- `gross_revenue = round(sum(order_amount), 2)` (DECIMAL)
- `unique_customers = countDistinct(customer_id)`

Idempotency:
- delete gold rows for `dt=<dt>` then append

Output:
- `gold.daily_product_sales`

---

## 🔎 Querying in Athena

Athena can query Iceberg tables via **AwsDataCatalog** (Glue Data Catalog).
Use Athena Engine v3 and set a query results location, e.g.:

- `s3://<bucket>/athena-results/`

Example queries:
```sql
SHOW DATABASES;
SHOW TABLES IN gold;

SELECT *
FROM gold.daily_product_sales
ORDER BY dt DESC
LIMIT 50;