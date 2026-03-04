from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------------------
# Spark Session
# ----------------------------
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Function to create silver stream
# ----------------------------
def create_silver_stream(input_path, output_path):

    df_bronze = spark.readStream \
        .format("delta") \
        .load(input_path)

# watermark saying if i see the same event id again , ignore it .
    df_cleaned = df_bronze \
        .filter(col("event_id").isNotNull()) \
        .filter(col("event_time").isNotNull()) \
        .withWatermark("event_time", "10 minutes") \
        .dropDuplicates(["event_id"])

    return df_cleaned.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .start(output_path)


# ----------------------------
# Start Both Silver Streams
# ----------------------------

query_user = create_silver_stream(
    "/tmp/delta/bronze/user_activity",
    "/tmp/delta/silver/user_activity"
)

query_order = create_silver_stream(
    "/tmp/delta/bronze/order_events",
    "/tmp/delta/silver/order_events"
)

query_user.awaitTermination()
query_order.awaitTermination()