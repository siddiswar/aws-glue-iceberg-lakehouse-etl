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

stg = spark.table("glue_catalog.bronze.customers_stg").where(F.col("dt") == F.to_date(F.lit(dt)))
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("ingest_ts").desc())
stg = (stg.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn"))

tracked = ["full_name", "email", "city"]
stg = (stg.withColumn("hash_diff",
                      F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked]),
                             256))
       .select("customer_id", "full_name", "email", "city", "hash_diff", "ingest_ts")
       .dropDuplicates(["customer_id"]))
stg.createOrReplaceTempView("stg")

spark.sql("""MERGE INTO glue_catalog.silver.dim_customer_scd2 tgt
USING stg src
ON tgt.customer_id=src.customer_id AND tgt.is_current=true
WHEN MATCHED AND tgt.hash_diff <> src.hash_diff THEN
  UPDATE SET tgt.is_current=false, tgt.effective_to_ts=current_timestamp()
""")
spark.sql("""INSERT INTO glue_catalog.silver.dim_customer_scd2
SELECT src.customer_id, src.full_name, src.email, src.city, src.hash_diff,
       current_timestamp() AS effective_from_ts,
       CAST(NULL AS timestamp) AS effective_to_ts,
       true AS is_current,
       src.ingest_ts
FROM stg src
LEFT JOIN glue_catalog.silver.dim_customer_scd2 tgt
  ON tgt.customer_id=src.customer_id AND tgt.is_current=true
WHERE tgt.customer_id IS NULL OR tgt.hash_diff <> src.hash_diff
""")
