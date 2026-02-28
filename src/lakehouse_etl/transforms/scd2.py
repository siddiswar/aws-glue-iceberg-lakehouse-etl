from __future__ import annotations

from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


def latest_per_key(stg: DataFrame, key_col: str, order_cols: Sequence[str]) -> DataFrame:
    order_exprs = [F.col(c).desc_nulls_last() for c in order_cols]
    w = Window.partitionBy(key_col).orderBy(*order_exprs)
    return stg.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def add_hash_diff(df: DataFrame, tracked_cols: Iterable[str], hash_col: str = "hash_diff") -> DataFrame:
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]
    return df.withColumn(hash_col, F.sha2(F.concat_ws("||", *exprs), 256))


def scd2_merge(
        spark: SparkSession,
        stg: DataFrame,
        target_table: str,
        key_col: str,
        tracked_cols: Sequence[str],
        order_cols: Sequence[str],
) -> None:
    """
    Generic SCD2 merge for Iceberg table.
    Expects target table schema:
      key_col, tracked_cols..., hash_diff, effective_from_ts, effective_to_ts, is_current, ingest_ts
    """
    stg_latest = latest_per_key(stg, key_col=key_col, order_cols=order_cols)
    stg_h = add_hash_diff(stg_latest, tracked_cols=tracked_cols)

    select_cols = [key_col] + list(tracked_cols) + ["hash_diff", "ingest_ts"]
    stg_view = "stg"
    stg_h.select(*select_cols).dropDuplicates([key_col]).createOrReplaceTempView(stg_view)

    # Expire changed current rows
    spark.sql(f"""MERGE INTO {target_table} tgt
    USING {stg_view} src
    ON tgt.{key_col} = src.{key_col} AND tgt.is_current = true
    WHEN MATCHED AND tgt.hash_diff <> src.hash_diff THEN
      UPDATE SET tgt.is_current = false, tgt.effective_to_ts = current_timestamp()
    """)

    # Insert new current rows (new keys or changed keys)
    tracked_select = ", ".join([f"src.{c}" for c in tracked_cols])
    spark.sql(f"""INSERT INTO {target_table}
    SELECT
      src.{key_col},
      {tracked_select},
      src.hash_diff,
      current_timestamp() AS effective_from_ts,
      CAST(NULL AS timestamp) AS effective_to_ts,
      true AS is_current,
      src.ingest_ts
    FROM {stg_view} src
    LEFT JOIN {target_table} tgt
      ON tgt.{key_col} = src.{key_col} AND tgt.is_current = true
    WHERE tgt.{key_col} IS NULL OR tgt.hash_diff <> src.hash_diff
    """)
