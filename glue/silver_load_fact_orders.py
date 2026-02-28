import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

from lakehouse_etl.transforms.fact_orders import transform_fact_orders

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt"])
dt = args["dt"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

orders = spark.table("glue_catalog.bronze.orders_stg").where(F.col("dt") == F.to_date(F.lit(dt)))

out_df = transform_fact_orders(orders)

# Idempotent per day
spark.sql(f"DELETE FROM glue_catalog.silver.fact_orders WHERE dt = DATE('{dt}')")
out_df.writeTo("glue_catalog.silver.fact_orders").append()
