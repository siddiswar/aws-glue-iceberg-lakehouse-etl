***

## S3 layout

### Raw (CSV landing zone)
Daily files are written under:

- `s3://<bucket>/raw/customers/dt=YYYY-MM-DD/customers.csv`
- `s3://<bucket>/raw/products/dt=YYYY-MM-DD/products.csv`
- `s3://<bucket>/raw/orders/dt=YYYY-MM-DD/orders.csv`

### Warehouse (Iceberg tables)
Iceberg tables live under:

- `s3://<bucket>/warehouse/bronze/...`
- `s3://<bucket>/warehouse/silver/...`
- `s3://<bucket>/warehouse/gold/...`

Iceberg stores **Parquet data files** plus **metadata** (snapshots, manifests) in these locations.

***

## DynamoDB tables

### 1) `etl_generator_state`

Used for **stateful raw data generation**:

- **Run marker (idempotency):** `pk = run#YYYY-MM-DD`
    - prevents accidental re-generation of the same day
- **Counters (monotonic IDs):**
    - `pk = counter#customers` → `next_seq`
    - `pk = counter#products` → `next_seq`
    - `pk = counter#orders` → `next_seq`

### 2) `etl_dim_store`

Maintains a **current dimension snapshot** so we can simulate realistic SCD2 changes:

- Partition key: `entity` (`customer` or `product`)
- Sort key: `id` (`C000001`, `P000001`, ...)

This lets each day’s raw CSV contain consistent entities, with a small percentage of changes to trigger SCD2.

> **Important DynamoDB note:** Python `float` values cannot be written to DynamoDB via boto3; numeric values must be
> stored as `Decimal`. The generator Lambda converts floats to `Decimal` before writing.

***

## Lambdas

### A) `compute_yesterday_dt`

**Purpose:** Determine the `dt` partition to process.

- Uses timezone `Europe/London`
- Computes **yesterday’s date** as `YYYY-MM-DD`
- Returns JSON: `{ "dt": "YYYY-MM-DD" }`

This value is passed through the entire Step Functions workflow.

### B) `generate_raw_csvs`

**Purpose:** Automatically generate and inject raw daily CSV files into S3.

This is the “data producer” for learning/testing. It replaces a real upstream source system.

#### What it does (high-level)

For a given input `dt`:

1) **Idempotency / run marker**
    - Attempts to create `run#<dt>` in `etl_generator_state`.
    - If the run marker already exists, the function normally skips regeneration.

2) **Self-healing behavior (important)**
    - If DynamoDB indicates the day was generated but the S3 raw files are missing (e.g., you emptied the bucket),
      the Lambda will **regenerate** the raw files.
    - It does this by:
        - checking for the three expected S3 keys
        - deleting the `run#<dt>` marker if files are missing
        - recreating the run marker and continuing

3) **Dimension seeding (first run only)**
    - If `etl_dim_store` is empty:
        - creates an initial set of customers and products
        - generates IDs using DynamoDB atomic counters
            - customers: `C000001`, `C000002`, ...
            - products: `P000001`, `P000002`, ...

4) **SCD2-like changes**
    - Randomly changes a small % of customer/product attributes (e.g., city, category, price)
    - Updates the **current snapshot** in `etl_dim_store`
    - These changes will later be captured as new SCD2 versions in the Silver dimension tables.

5) **Daily raw CSV snapshot export**
    - Exports full current snapshots to raw CSV:
        - customers.csv: `customer_id, full_name, email, city, updated_at`
        - products.csv: `product_id, product_name, category, list_price, updated_at`

6) **Orders generation (append-only)**
    - Allocates a block of order IDs using the DynamoDB atomic counter:
        - `order_id = O<yyyymmdd>-<seq>`
    - Creates N orders for the day referencing existing customers/products:
        - `order_id, order_ts, customer_id, product_id, quantity, unit_price`

7) **Write to S3** under `raw/<entity>/dt=<dt>/...`

#### Output

Returns a small JSON payload including `dt` and bucket (and can include status).

***

## Glue jobs and transformation logic

All Glue jobs use:

- Glue version: **4.0**
- Iceberg enabled via `--datalake-formats=iceberg`
- Glue Catalog + Iceberg Spark extensions

### 0) `init_iceberg_tables`

**Purpose:** Create the Glue databases and Iceberg tables if they don’t exist.

Creates (if missing):

- Databases: `bronze`, `silver`, `gold`
- Iceberg tables:
    - `bronze.customers_stg`, `bronze.products_stg`, `bronze.orders_stg`
    - `silver.dim_customer_scd2`, `silver.dim_product_scd2`, `silver.fact_orders`
    - `gold.daily_product_sales`

### 1) `bronze_ingest_customers`

**Input:** `raw/customers/dt=<dt>/customers.csv`

**Transform rules:**

- Trim strings
- Normalize email: `lower(trim(email))`
- Parse `updated_at` timestamp
- Drop records with missing `customer_id`
- Add:
    - `dt` (date partition)
    - `ingest_ts = current_timestamp()`

**Output:** append to Iceberg table `bronze.customers_stg` partitioned by `dt`

### 2) `bronze_ingest_products`

**Input:** `raw/products/dt=<dt>/products.csv`

**Transform rules:**

- Trim strings
- Parse `updated_at` timestamp
- Drop records with missing `product_id`
- Ensure `list_price` is numeric
- Add `dt`, `ingest_ts`

**Output:** `bronze.products_stg`

### 3) `bronze_ingest_orders`

**Input:** `raw/orders/dt=<dt>/orders.csv`

**Transform rules:**

- Parse `order_ts` timestamp
- Drop records with missing `order_id`
- Enforce `quantity > 0`
- Add `dt`, `ingest_ts`

**Output:** `bronze.orders_stg`

### 4) `silver_merge_dim_customer_scd2` (SCD Type 2)

**Input:** `bronze.customers_stg` for `dt=<dt>`

**Tracked columns:** `full_name, email, city`

**Logic:**

- Compute `hash_diff` = SHA-256 hash of tracked columns
- For each `customer_id`:
    - If there is a current row and `hash_diff` changed → expire current row:
        - `is_current=false`, `effective_to_ts=current_timestamp()`
    - Insert new current row with:
        - `effective_from_ts=current_timestamp()`, `effective_to_ts=NULL`, `is_current=true`

**Output:** `silver.dim_customer_scd2`

### 5) `silver_merge_dim_product_scd2` (SCD Type 2)

**Input:** `bronze.products_stg` for `dt=<dt>`

**Tracked columns:** `product_name, category, list_price`

Same SCD2 logic as customer dim.

**Output:** `silver.dim_product_scd2`

### 6) `silver_load_fact_orders` (append-only incremental)

**Input:** `bronze.orders_stg` for `dt=<dt>`

**Logic:**

- De-duplicate within day by `order_id`
- Compute `order_amount = round(quantity * unit_price, 2)` as `DECIMAL(18,2)`
- Idempotency for day:
    - delete existing `dt` partition in `silver.fact_orders`
    - append the day’s data

**Output:** `silver.fact_orders`

### 7) `gold_build_daily_product_sales` (target aggregate)

**Input:**

- `silver.fact_orders` for `dt=<dt>`
- **current** product dimension: `silver.dim_product_scd2` where `is_current=true`

**Join:** `fact.product_id = dim.product_id` (current dim)

**Aggregations (per dt + product):**

- `orders_count = countDistinct(order_id)`
- `units_sold = sum(quantity)`
- `gross_revenue = round(sum(order_amount), 2)` (DECIMAL)
- `unique_customers = countDistinct(customer_id)`

**Idempotency:** delete `dt` in gold table then append.

**Output:** `gold.daily_product_sales`

***

## Step Functions state management (important)

When using `glue:startJobRun.sync`, the Glue job run output can replace the entire state.
To preserve `dt` across steps, each Glue task stores its output under a `ResultPath`, e.g.:

- `ResultPath: $.bronze_customers`
- `ResultPath: $.merge_product`

This ensures `$.dt` is always available for the next task.

***

## Deploy

### Prerequisites

- AWS CLI configured for your account
- SAM CLI installed

### Deploy (non-interactive)

Use the deploy script:

```bash
./scripts/deploy_stack.sh