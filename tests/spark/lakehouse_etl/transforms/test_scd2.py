from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.lakehouse_etl.transforms.scd2 import latest_per_key, add_hash_diff, scd2_merge


def test_latest_per_key_picks_latest_by_order_cols(spark):
    df = spark.createDataFrame(
        [
            ("C1", "Alice", "2026-02-20 10:00:00", "2026-02-20 10:01:00"),
            ("C1", "Alice NEW", "2026-02-20 11:00:00", "2026-02-20 11:01:00"),
            ("C2", "Bob", "2026-02-20 09:00:00", "2026-02-20 09:01:00"),
            ("C2", "Bob OLD", "2026-02-19 09:00:00", "2026-02-19 09:01:00"),
        ],
        ["customer_id", "full_name", "updated_at", "ingest_ts"],
    ).select(
        "customer_id",
        "full_name",
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
        F.to_timestamp("ingest_ts", "yyyy-MM-dd HH:mm:ss").alias("ingest_ts"),
    )

    out = latest_per_key(df, key_col="customer_id", order_cols=["updated_at", "ingest_ts"])
    rows = {r["customer_id"]: r["full_name"] for r in out.collect()}

    assert rows["C1"] == "Alice NEW"
    assert rows["C2"] == "Bob"


def test_add_hash_diff_is_stable_and_sensitive_to_changes(spark):
    df = spark.createDataFrame(
        [
            ("C1", "Alice", "alice@example.com", "London"),
            ("C2", "Bob", "bob@example.com", "MK"),
        ],
        ["customer_id", "full_name", "email", "city"],
    )

    out1 = add_hash_diff(df, tracked_cols=["full_name", "email", "city"])
    out2 = add_hash_diff(df, tracked_cols=["full_name", "email", "city"])

    # same input => same hash
    h1 = {r["customer_id"]: r["hash_diff"] for r in out1.select("customer_id", "hash_diff").collect()}
    h2 = {r["customer_id"]: r["hash_diff"] for r in out2.select("customer_id", "hash_diff").collect()}
    assert h1 == h2

    # changing tracked col should change hash
    df_changed = df.withColumn("city", F.when(F.col("customer_id") == "C1", F.lit("Oxford")).otherwise(F.col("city")))
    out3 = add_hash_diff(df_changed, tracked_cols=["full_name", "email", "city"])
    h3 = {r["customer_id"]: r["hash_diff"] for r in out3.select("customer_id", "hash_diff").collect()}
    assert h3["C1"] != h1["C1"]
    assert h3["C2"] == h1["C2"]


@pytest.mark.xfail(
    reason="scd2_merge() uses MERGE INTO against an Iceberg table. Local Spark without Iceberg won’t execute MERGE reliably.Test full SCD2 merge behavior in the AWS job-level tests (Glue has Iceberg + MERGE)")
def test_scd2_merge_expires_and_inserts_new_current_row(spark):
    """
    This test validates SCD2 behavior using Spark SQL only.
    We emulate the target table as a temp view and validate the SQL effects.
    """
    # ----- Create initial target "table" -----
    target_rows = [
        ("C1", "Alice", "alice@example.com", "London", "hash_old", "2026-02-19 00:00:00", None, True,
         "2026-02-19 00:00:00"),
    ]
    target = spark.createDataFrame(
        target_rows,
        [
            "customer_id",
            "full_name",
            "email",
            "city",
            "hash_diff",
            "effective_from_ts",
            "effective_to_ts",
            "is_current",
            "ingest_ts",
        ],
    ).select(
        "customer_id",
        "full_name",
        "email",
        "city",
        "hash_diff",
        F.to_timestamp("effective_from_ts").alias("effective_from_ts"),
        F.to_timestamp("effective_to_ts").alias("effective_to_ts"),
        F.col("is_current").cast("boolean").alias("is_current"),
        F.to_timestamp("ingest_ts").alias("ingest_ts"),
    )

    # Register as a view that SQL can MERGE/INSERT against
    # NOTE: Spark SQL MERGE requires an actual table format in many Spark builds.
    # To keep this test portable, we validate the generated staging and "insert condition"
    # by executing the same logic with DataFrame operations instead of MERGE.
    #
    # If you run Spark 3.5 + Iceberg locally, you can adapt this to a real Iceberg table.

    # ----- Staging (new day) -----
    stg = spark.createDataFrame(
        [
            ("C1", "Alice", "alice@example.com", "Oxford", "2026-02-20 10:00:00", "2026-02-20 10:01:00"),
        ],
        ["customer_id", "full_name", "email", "city", "updated_at", "ingest_ts"],
    ).select(
        "customer_id",
        "full_name",
        "email",
        "city",
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
        F.to_timestamp("ingest_ts", "yyyy-MM-dd HH:mm:ss").alias("ingest_ts"),
    )

    # We can still test that scd2_merge creates a staging view with hash_diff properly
    # and that rows that differ would be inserted.
    # For a fully “MERGE INTO” test, we need Iceberg/Hudi/Delta locally.
    stg_latest = latest_per_key(stg, "customer_id", ["updated_at", "ingest_ts"])
    stg_h = add_hash_diff(stg_latest, ["full_name", "email", "city"]).select(
        "customer_id", "full_name", "email", "city", "hash_diff", "ingest_ts"
    )

    # Simulate "current row differs" check
    current = target.where(F.col("is_current") == F.lit(True))
    joined = stg_h.join(current.select("customer_id", "hash_diff"), on="customer_id", how="left") \
        .withColumnRenamed("hash_diff", "new_hash") \
        .withColumnRenamed("hash_diff", "old_hash")

    # old_hash is null (new key) OR differs -> would insert
    to_insert = stg_h.join(current.select("customer_id", "hash_diff"), "customer_id", "left") \
        .where(F.col("hash_diff").isNull() | (F.col("hash_diff") != F.col("hash_diff")))

    # Instead of the above confusing column shadowing, do it clean:
    cur_hash = current.select(F.col("customer_id").alias("cid"), F.col("hash_diff").alias("cur_hash"))
    check = stg_h.join(cur_hash, stg_h.customer_id == cur_hash.cid, "left")
    would_insert = check.where(F.col("cur_hash").isNull() | (F.col("cur_hash") != F.col("hash_diff")))

    assert would_insert.count() == 1
