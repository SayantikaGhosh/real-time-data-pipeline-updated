from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Schema
# ----------------------------
schema = StructType() \
    .add("event_id", StringType()) \
    .add("session_id", StringType()) \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("price", FloatType()) \
    .add("event_time", StringType()) \
    .add("ingestion_time", StringType())

# ----------------------------
# Bronze Stream Creator
# ----------------------------
def create_bronze_stream(topic, output_path):

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()

    df_parsed = df_raw \
        .selectExpr(
            "CAST(value AS STRING) as json",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        ) \
        .select(
            from_json(col("json"), schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "topic", "partition", "offset", "kafka_timestamp") \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))

    return df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .start(output_path)


# ----------------------------
# Start Both Streams
# ----------------------------
create_bronze_stream(
    "user_activity_events",
    "/tmp/delta/bronze/user_activity"
)

create_bronze_stream(
    "order_events",
    "/tmp/delta/bronze/order_events"
)

spark.streams.awaitAnyTermination()