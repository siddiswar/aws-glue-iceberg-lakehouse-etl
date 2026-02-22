import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt"])
dt = args["dt"]
sc = SparkContext();
glueContext = GlueContext(sc);
spark = glueContext.spark_session

orders = (spark.table("glue_catalog.bronze.orders_stg").where(F.col("dt") == F.to_date(F.lit(dt)))
          .dropDuplicates(["order_id"])
          .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
          .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
          .filter(F.col("unit_price").isNotNull()))
orders = orders.withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2).cast(DecimalType(18, 2)))

spark.sql(f"DELETE FROM glue_catalog.silver.fact_orders WHERE dt = DATE('{dt}')")
orders.select("order_id", "order_ts", "customer_id", "product_id", "quantity", "unit_price", "order_amount", "dt",
              "ingest_ts").writeTo("glue_catalog.silver.fact_orders").append()
