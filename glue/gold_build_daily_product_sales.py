import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

from lakehouse_etl.transforms.gold import build_daily_product_sales

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt"])
dt = args["dt"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

fact = spark.table("glue_catalog.silver.fact_orders").where(F.col("dt") == F.to_date(F.lit(dt)))
dimP = spark.table("glue_catalog.silver.dim_product_scd2").where(F.col("is_current") == F.lit(True))

agg = build_daily_product_sales(fact, dimP)

spark.sql(f"DELETE FROM glue_catalog.gold.daily_product_sales WHERE dt = DATE('{dt}')")
agg.writeTo("glue_catalog.gold.daily_product_sales").append()
