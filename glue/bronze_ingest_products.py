import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt", "bucket", "raw_prefix"])
dt = args["dt"];
bucket = args["bucket"];
raw_prefix = args["raw_prefix"].rstrip("/")
raw_path = f"s3://{bucket}/{raw_prefix}/dt={dt}/products.csv"
ts_fmt = "yyyy-MM-dd HH:mm:ss"

sc = SparkContext();
glueContext = GlueContext(sc);
spark = glueContext.spark_session
df = spark.read.option("header", "true").csv(raw_path)

out = (df.select("product_id", "product_name", "category", "list_price", "updated_at")
       .withColumn("product_id", F.trim("product_id"))
       .withColumn("product_name", F.trim("product_name"))
       .withColumn("category", F.trim("category"))
       .withColumn("list_price", F.col("list_price").cast("double"))
       .withColumn("updated_at", F.to_timestamp("updated_at", ts_fmt))
       .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
       .withColumn("dt", F.to_date(F.lit(dt)))
       .withColumn("ingest_ts", F.current_timestamp()))
out.writeTo("glue_catalog.bronze.products_stg").append()
