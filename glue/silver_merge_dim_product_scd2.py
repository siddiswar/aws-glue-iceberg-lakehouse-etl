import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "dt"])
dt = args["dt"]
sc = SparkContext();
glueContext = GlueContext(sc);
spark = glueContext.spark_session

stg = spark.table("glue_catalog.bronze.products_stg").where(F.col("dt") == F.to_date(F.lit(dt)))
w = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("ingest_ts").desc())
stg = (stg.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn"))

stg = (stg.withColumn("hash_diff", F.sha2(F.concat_ws("||",
                                                      F.coalesce(F.col("product_name").cast("string"), F.lit("")),
                                                      F.coalesce(F.col("category").cast("string"), F.lit("")),
                                                      F.coalesce(F.col("list_price").cast("string"), F.lit(""))
                                                      ), 256))
       .select("product_id", "product_name", "category", "list_price", "hash_diff", "ingest_ts")
       .dropDuplicates(["product_id"]))
stg.createOrReplaceTempView("stg")

spark.sql("""MERGE INTO glue_catalog.silver.dim_product_scd2 tgt
USING stg src
ON tgt.product_id=src.product_id AND tgt.is_current=true
WHEN MATCHED AND tgt.hash_diff <> src.hash_diff THEN
  UPDATE SET tgt.is_current=false, tgt.effective_to_ts=current_timestamp()
""")
spark.sql("""INSERT INTO glue_catalog.silver.dim_product_scd2
SELECT src.product_id, src.product_name, src.category, src.list_price, src.hash_diff,
       current_timestamp() AS effective_from_ts,
       CAST(NULL AS timestamp) AS effective_to_ts,
       true AS is_current,
       src.ingest_ts
FROM stg src
LEFT JOIN glue_catalog.silver.dim_product_scd2 tgt
  ON tgt.product_id=src.product_id AND tgt.is_current=true
WHERE tgt.product_id IS NULL OR tgt.hash_diff <> src.hash_diff
""")
