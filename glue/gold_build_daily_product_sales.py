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

fact = spark.table("glue_catalog.silver.fact_orders").where(F.col("dt") == F.to_date(F.lit(dt)))
dimP = spark.table("glue_catalog.silver.dim_product_scd2").where(F.col("is_current") == F.lit(True))
enriched = fact.join(dimP.select("product_id", "category"), on="product_id", how="left")

agg = (enriched.groupBy("dt", "product_id", "category")
       .agg(F.countDistinct("order_id").alias("orders_count"),
            F.sum("quantity").cast("bigint").alias("units_sold"),
            F.round(F.sum("order_amount"), 2).cast(DecimalType(18, 2)).alias("gross_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"))
       .withColumn("ingest_ts", F.current_timestamp()))

spark.sql(f"DELETE FROM glue_catalog.gold.daily_product_sales WHERE dt = DATE('{dt}')")
agg.writeTo("glue_catalog.gold.daily_product_sales").append()
