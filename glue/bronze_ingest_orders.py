import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt", "bucket", "raw_prefix"])
dt = args["dt"];
bucket = args["bucket"];
raw_prefix = args["raw_prefix"].rstrip("/")
raw_path = f"s3://{bucket}/{raw_prefix}/dt={dt}/orders.csv"
ts_fmt = "yyyy-MM-dd HH:mm:ss"

sc = SparkContext();
glueContext = GlueContext(sc);
spark = glueContext.spark_session
df = spark.read.option("header", "true").csv(raw_path)

out = (df.select("order_id", "order_ts", "customer_id", "product_id", "quantity", "unit_price")
       .withColumn("order_id", F.trim("order_id"))
       .withColumn("customer_id", F.trim("customer_id"))
       .withColumn("product_id", F.trim("product_id"))
       .withColumn("quantity", F.col("quantity").cast("int"))
       .withColumn("unit_price", F.col("unit_price").cast("double"))
       .withColumn("order_ts", F.to_timestamp("order_ts", ts_fmt))
       .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
       .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
       .withColumn("dt", F.to_date(F.lit(dt)))
       .withColumn("ingest_ts", F.current_timestamp()))
out.writeTo("glue_catalog.bronze.orders_stg").append()
