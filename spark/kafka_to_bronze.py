from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType
import time

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
# Delta extensions are configured here explicitly rather than relying solely
# on spark-submit --packages flags. This makes the job self-contained and
# prevents silent failures if the flags are ever missing.
#
# spark.sql.shuffle.partitions = 2
#   Default is 200. With 1 Kafka partition and ~11k events over 10 minutes,
#   200 shuffle partitions means 198 empty partitions each trying to write
#   their own tiny parquet file. Setting to 2 keeps file count sane while
#   still allowing minimal parallelism.

spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# SCHEMA
# ---------------------------------------------------------------------------
# Must exactly match the event structure produced by app.py.
# Note: ingestion_time has been renamed to producer_time — it represents
# when the producer created and sent the event, NOT when Kafka received it.
# The true ingestion time is kafka_timestamp, captured from the Kafka metadata
# below. Having both lets you measure producer → Kafka lag.

schema = StructType() \
    .add("event_id",      StringType()) \
    .add("session_id",    StringType()) \
    .add("user_id",       StringType()) \
    .add("event_type",    StringType()) \
    .add("product_id",    StringType()) \
    .add("product_name",  StringType()) \
    .add("category",      StringType()) \
    .add("price",         FloatType()) \
    .add("event_time",    StringType()) \
    .add("producer_time", StringType())   # renamed from ingestion_time

# ---------------------------------------------------------------------------
# BRONZE STREAM CREATOR
# ---------------------------------------------------------------------------

def create_bronze_stream(topic, output_path):
    """
    Reads raw events from a Kafka topic and writes them as-is to the
    Bronze Delta layer. Bronze is the raw, unfiltered, complete record
    of everything that came through Kafka. No transformations, no filtering.

    Args:
        topic:       Kafka topic name to subscribe to
        output_path: Delta table path to write bronze data
    """

    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 6000) \
        .load()

    # ---------------------------------------------------------------------------
    # PARSE KAFKA MESSAGE
    # ---------------------------------------------------------------------------
    # Kafka delivers messages as raw bytes. We:
    # 1. Cast the value bytes to a string (the JSON payload)
    # 2. Keep useful Kafka metadata: topic, partition, offset, kafka_timestamp
    #    kafka_timestamp is the TRUE ingestion time — when Kafka received the msg
    # 3. Parse the JSON string into typed columns using our schema
    # 4. Convert event_time and producer_time from ISO string to proper timestamps

    df_parsed = df_raw \
        .selectExpr(
            "CAST(value AS STRING) as json",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"    # true ingestion time from broker
        ) \
        .select(
            from_json(col("json"), schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "topic", "partition", "offset", "kafka_timestamp") \
        .withColumn("event_time",    to_timestamp(col("event_time"))) \
        .withColumn("producer_time", to_timestamp(col("producer_time")))  # renamed

    # ---------------------------------------------------------------------------
    # WRITE TO BRONZE DELTA TABLE
    # ---------------------------------------------------------------------------
    # trigger(processingTime="5 minutes"):
    #   Instead of firing every 500ms (default), Spark waits 5 minutes to
    #   accumulate data before writing. This means:
    #   - 10 minute run → ~2 trigger fires → ~2-4 parquet files per stream
    #   - Each file contains ~5 minutes worth of events (~9,000 events)
    #   - File size roughly 500KB-1MB instead of bytes
    #   This is the single most impactful fix for the small files problem.
    #
    # noDataMicroBatches.enabled = false:
    #   Stops Spark from firing empty triggers when Kafka has no new messages.
    #   Without this, Spark writes an empty parquet file every 5 minutes
    #   indefinitely even after the producer stops — creating unnecessary files.
    #
    # startingOffsets = "earliest":
    #   Bronze must be complete. If Spark restarts mid-run, it picks up from
    #   its checkpoint and won't miss any messages. "latest" would silently
    #   skip everything produced during the downtime.

    return df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .start(output_path)


# ---------------------------------------------------------------------------
# START BOTH STREAMS
# ---------------------------------------------------------------------------

query_user = create_bronze_stream(
    "user_activity_events",
    "/tmp/delta/bronze/user_activity"
)

query_order = create_bronze_stream(
    "order_events",
    "/tmp/delta/bronze/order_events"
)

# ---------------------------------------------------------------------------
# TERMINATION HANDLING
# ---------------------------------------------------------------------------
# awaitAnyTermination() was the original approach — it unblocks as soon as
# ANY stream dies, meaning the other stream silently stops being monitored.
#
# Instead we run a monitoring loop that checks both streams every 30 seconds.
# If either stream dies unexpectedly, we log it clearly and terminate the
# entire job so the failure is visible in Docker logs and Grafana.

print("[INFO] Both bronze streams running. Monitoring...")

while True:
    time.sleep(30)

    if not query_user.isActive:
        print("[ERROR] user_activity bronze stream died unexpectedly.")
        print(f"[ERROR] Last exception: {query_user.exception()}")
        query_order.stop()
        break

    if not query_order.isActive:
        print("[ERROR] order_events bronze stream died unexpectedly.")
        print(f"[ERROR] Last exception: {query_order.exception()}")
        query_user.stop()
        break

    # Progress log every 30 seconds so you can see it's alive in Docker logs
    u_progress = query_user.lastProgress
    o_progress = query_order.lastProgress

    u_rows = u_progress["numInputRows"] if u_progress else 0
    o_rows = o_progress["numInputRows"] if o_progress else 0

    print(f"[PROGRESS] user_activity: {u_rows} rows in last batch | "
          f"order_events: {o_rows} rows in last batch")

print("[INFO] Bronze streaming job terminated.")