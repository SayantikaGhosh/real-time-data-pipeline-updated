from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
bronze_path = "/tmp/delta/bronze"

# Wait for Bronze table
for _ in range(10):
    try:
        spark.read.format("delta").load(bronze_path).limit(1).collect()
        break
    except:
        time.sleep(5)
else:
    print("Bronze table not ready. Exiting.")
    spark.stop()
    exit(1)

# Read Bronze stream
df_bronze = spark.readStream.format("delta").load(bronze_path)

# Clean & deduplicate
df_silver = df_bronze.filter(
    col("user_id").isNotNull() &
    col("product_id").isNotNull() &
    col("event_type").isin("click", "purchase") &
    (col("price") > 0)
).dropDuplicates(["event_id"])

# Write Silver Delta table
df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/silver/_checkpoint") \
    .start("/tmp/delta/silver") \
    .awaitTermination()
