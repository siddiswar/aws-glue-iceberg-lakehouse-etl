import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

from lakehouse_etl.transforms.scd2 import scd2_merge

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt"])
dt = args["dt"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

stg = spark.table("glue_catalog.bronze.products_stg").where(F.col("dt") == F.to_date(F.lit(dt)))

scd2_merge(
    spark=spark,
    stg=stg,
    target_table="glue_catalog.silver.dim_product_scd2",
    key_col="product_id",
    tracked_cols=["product_name", "category", "list_price"],
    order_cols=["updated_at", "ingest_ts"],
)
