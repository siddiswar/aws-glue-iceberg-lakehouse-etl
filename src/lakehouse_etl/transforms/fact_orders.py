from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType


def transform_fact_orders(orders_bronze: DataFrame) -> DataFrame:
    # De-dup by order_id within the day and compute order_amount
    df = orders_bronze.dropDuplicates(["order_id"])
    df = df.withColumn(
        "order_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2).cast(DecimalType(18, 2)),
    )
    return df
