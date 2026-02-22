import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt", "bucket", "raw_prefix"])
dt = args["dt"];
bucket = args["bucket"];
raw_prefix = args["raw_prefix"].rstrip("/")
raw_path = f"s3://{bucket}/{raw_prefix}/dt={dt}/customers.csv"
ts_fmt = "yyyy-MM-dd HH:mm:ss"

sc = SparkContext();
glueContext = GlueContext(sc);
spark = glueContext.spark_session
df = spark.read.option("header", "true").csv(raw_path)

out = (df.select("customer_id", "full_name", "email", "city", "updated_at")
       .withColumn("customer_id", F.trim("customer_id"))
       .withColumn("full_name", F.trim("full_name"))
       .withColumn("email", F.lower(F.trim("email")))
       .withColumn("city", F.trim("city"))
       .withColumn("updated_at", F.to_timestamp("updated_at", ts_fmt))
       .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
       .withColumn("dt", F.to_date(F.lit(dt)))
       .withColumn("ingest_ts", F.current_timestamp()))
out.writeTo("glue_catalog.bronze.customers_stg").append()
