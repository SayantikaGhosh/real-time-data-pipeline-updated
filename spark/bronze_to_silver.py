from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
# Delta extensions configured explicitly — same as bronze.
# shuffle.partitions = 2 for the same reason: 1 Kafka partition + low data
# volume means 200 default partitions creates hundreds of empty parquet files.

spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# VALID VALUES FOR VALIDATION
# ---------------------------------------------------------------------------
# Silver is the cleaned, validated layer. We reject events that don't
# conform to the known schema of our ecommerce pipeline.

VALID_EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]

# ---------------------------------------------------------------------------
# SILVER STREAM CREATOR
# ---------------------------------------------------------------------------

def create_silver_stream(input_path, output_path):
    """
    Reads from a Bronze Delta table, cleans and validates the data,
    deduplicates by event_id, and writes to a Silver Delta table.

    Validation rules applied:
    - event_id must not be null (can't track without an ID)
    - event_time must not be null (can't do time-based aggregations without it)
    - price must be a positive number (negative/zero prices are corrupt data)
    - product_id must not be null
    - event_type must be one of the known valid values

    Args:
        input_path:  Bronze Delta table path to read from
        output_path: Silver Delta table path to write to
    """

    df_bronze = spark.readStream \
        .format("delta") \
        .load(input_path)

    # ---------------------------------------------------------------------------
    # WATERMARK MUST COME BEFORE dropDuplicates
    # ---------------------------------------------------------------------------
    # withWatermark defines the event-time boundary Spark uses to decide when
    # a window is complete and state can be dropped. dropDuplicates uses this
    # boundary to know how long to keep event_ids in memory for dedup checking.
    #
    # If withWatermark comes AFTER dropDuplicates, the watermark has no effect
    # on the deduplication window — Spark treats it as an unbounded state store,
    # meaning it keeps ALL event_ids in memory forever, eventually causing OOM.
    #
    # Correct order: watermark → filters → dropDuplicates

    df_cleaned = df_bronze \
        .withWatermark("event_time", "10 minutes") \
        .filter(col("event_id").isNotNull()) \
        .filter(col("event_time").isNotNull()) \
        .filter(col("product_id").isNotNull()) \
        .filter(col("price").isNotNull() & (col("price") > 0)) \
        .filter(col("event_type").isin(VALID_EVENT_TYPES)) \
        .dropDuplicates(["event_id"])

    # ---------------------------------------------------------------------------
    # WRITE TO SILVER DELTA TABLE
    # ---------------------------------------------------------------------------
    # Same 5-minute trigger as bronze for consistent file sizes.
    # outputMode append is correct here — we're only adding new cleaned rows,
    # never updating or replacing existing ones.

    return df_cleaned.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .start(output_path)


# ---------------------------------------------------------------------------
# START BOTH STREAMS
# ---------------------------------------------------------------------------

query_user = create_silver_stream(
    "/tmp/delta/bronze/user_activity",
    "/tmp/delta/silver/user_activity"
)

query_order = create_silver_stream(
    "/tmp/delta/bronze/order_events",
    "/tmp/delta/silver/order_events"
)

# ---------------------------------------------------------------------------
# TERMINATION HANDLING
# ---------------------------------------------------------------------------
# Same monitoring loop pattern as bronze.
# query_user.awaitTermination() followed by query_order.awaitTermination()
# was the original approach — the second line never executes because the
# first blocks forever. If query_order died silently, you'd never know.
#
# This loop checks both streams every 30 seconds and logs progress,
# making failures immediately visible in Docker logs.

print("[INFO] Both silver streams running. Monitoring...")

while True:
    time.sleep(30)

    if not query_user.isActive:
        print("[ERROR] user_activity silver stream died unexpectedly.")
        print(f"[ERROR] Last exception: {query_user.exception()}")
        query_order.stop()
        break

    if not query_order.isActive:
        print("[ERROR] order_events silver stream died unexpectedly.")
        print(f"[ERROR] Last exception: {query_order.exception()}")
        query_user.stop()
        break

    # Progress log every 30 seconds
    u_progress = query_user.lastProgress
    o_progress = query_order.lastProgress

    u_rows = u_progress["numInputRows"] if u_progress else 0
    o_rows = o_progress["numInputRows"] if o_progress else 0

    print(f"[PROGRESS] user_activity: {u_rows} rows in last batch | "
          f"order_events: {o_rows} rows in last batch")

print("[INFO] Silver streaming job terminated.")