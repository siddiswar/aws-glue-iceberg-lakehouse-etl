import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from lakehouse_etl.io.paths import raw_customers_path
from lakehouse_etl.transforms.bronze import transform_customers

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt", "bucket"])
dt = args["dt"]
bucket = args["bucket"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

path = raw_customers_path(bucket, dt)
raw_df = spark.read.option("header", "true").csv(path)

out_df = transform_customers(raw_df, dt)
out_df.writeTo("glue_catalog.bronze.customers_stg").append()
