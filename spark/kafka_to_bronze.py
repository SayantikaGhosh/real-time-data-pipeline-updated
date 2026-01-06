from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

# Schema matching producer events
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("user_name", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("product_name", StringType()) \
    .add("category", StringType()) \
    .add("brand", StringType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType())  

# Spark session with Delta + Kafka
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and convert timestamp
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))  

# Write to Bronze Delta table
df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/bronze/_checkpoint") \
    .start("/tmp/delta/bronze") \
    .awaitTermination()
