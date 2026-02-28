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

stg = spark.table("glue_catalog.bronze.customers_stg").where(F.col("dt") == F.to_date(F.lit(dt)))

scd2_merge(
    spark=spark,
    stg=stg,
    target_table="glue_catalog.silver.dim_customer_scd2",
    key_col="customer_id",
    tracked_cols=["full_name", "email", "city"],
    order_cols=["updated_at", "ingest_ts"],
)
