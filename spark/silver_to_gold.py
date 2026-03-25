from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    when,
    approx_count_distinct,
    lit,
    round as spark_round,
    window
)
import time

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
# shuffle.partitions = 2 — same reasoning as bronze and silver.
# Default 200 creates hundreds of empty parquet files for our data volume.

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# READ FROM SILVER
# ---------------------------------------------------------------------------

df_user   = spark.readStream.format("delta").load("/tmp/delta/silver/user_activity")
df_orders = spark.readStream.format("delta").load("/tmp/delta/silver/order_events")

# ---------------------------------------------------------------------------
# WATERMARK
# ---------------------------------------------------------------------------
# Silver allows 10-minute late arrivals. Gold's watermark must be wide enough
# to absorb events that silver held in its window before emitting them.
# 2 minutes (original) was too tight — events held by silver's 10-min window
# would arrive at gold and get immediately dropped.
# 5 minutes gives comfortable breathing room while keeping state manageable
# on your 8GB machine.

df_user   = df_user.withWatermark("event_time", "5 minutes")
df_orders = df_orders.withWatermark("event_time", "5 minutes")

# ---------------------------------------------------------------------------
# USER METRICS — GOLD
# ---------------------------------------------------------------------------
# Aggregates user behaviour events (view, click, add_to_cart) per product
# per 1-minute tumbling window.
#
# Columns kept for BI visualisation:
#   product_id, product_name, category   → for filtering and grouping in BI
#   window_start, window_end             → time axis for trend charts
#   view_count                           → top of funnel metric
#   click_count                          → engagement metric
#   add_to_cart_count                    → purchase intent metric
#   unique_users                         → reach metric
#   click_through_rate                   → engagement rate (click/view)
#   add_to_cart_rate                     → intent rate (add_to_cart/view)
#
# product_name flows directly from the silver stream — no join needed
# since app.py now includes it in every event payload.

user_gold = (
    df_user
    .groupBy(
        "product_id",
        "product_name",
        "category",
        window(col("event_time"), "1 minute")
    )
    .agg(
        spark_sum(when(col("event_type") == "view",        1).otherwise(0)).alias("view_count"),
        spark_sum(when(col("event_type") == "click",       1).otherwise(0)).alias("click_count"),
        spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        approx_count_distinct("user_id").alias("unique_users")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .drop("window")
    # Click-through rate: what % of viewers clicked
    # Rounded to 4 decimal places for clean BI display
    .withColumn(
        "click_through_rate",
        when(col("view_count") > 0,
             spark_round(col("click_count") / col("view_count"), 4)
        ).otherwise(lit(0.0))
    )
    # Add-to-cart rate: what % of viewers added to cart
    # Useful for measuring purchase intent beyond just clicks
    .withColumn(
        "add_to_cart_rate",
        when(col("view_count") > 0,
             spark_round(col("add_to_cart_count") / col("view_count"), 4)
        ).otherwise(lit(0.0))
    )
    # Clean column order for BI tools
    .select(
        "product_id",
        "product_name",
        "category",
        "window_start",
        "window_end",
        "view_count",
        "click_count",
        "add_to_cart_count",
        "unique_users",
        "click_through_rate",
        "add_to_cart_rate"
    )
)

# ---------------------------------------------------------------------------
# outputMode("append"):
#   Delta Lake streaming sink only supports append mode.
#   With append + 5-minute watermark + 1-minute windows, Spark waits until
#   the watermark passes the window end before emitting — meaning a window
#   from 10:00-10:01 is emitted at ~10:06. Slightly delayed but complete
#   and correct. Acceptable trade-off for a demo pipeline.
# ---------------------------------------------------------------------------

user_query = (
    user_gold.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="5 minutes")
    .option("checkpointLocation", "/tmp/delta/gold/user_metrics/_checkpoint")
    .start("/tmp/delta/gold/user_metrics")
)

# ---------------------------------------------------------------------------
# ORDER METRICS — GOLD
# ---------------------------------------------------------------------------
# Aggregates purchase events per product per 1-minute tumbling window.
#
# Columns kept for BI visualisation:
#   product_id, product_name, category   → for filtering and grouping in BI
#   window_start, window_end             → time axis for revenue trend charts
#   purchase_count                       → sales volume metric
#   total_revenue_usd                    → revenue metric
#   avg_order_value                      → average spend per purchase
#   unique_buyers                        → unique customer reach

order_gold = (
    df_orders
    .groupBy(
        "product_id",
        "product_name",
        "category",
        window(col("event_time"), "1 minute")
    )
    .agg(
        count("*").alias("purchase_count"),
        spark_round(spark_sum("price"), 2).alias("total_revenue_usd"),
        approx_count_distinct("user_id").alias("unique_buyers")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .drop("window")
    # Average order value: revenue per purchase in this window
    # Useful for spotting high-value product windows in Grafana
    .withColumn(
        "avg_order_value",
        when(col("purchase_count") > 0,
             spark_round(col("total_revenue_usd") / col("purchase_count"), 2)
        ).otherwise(lit(0.0))
    )
    # Clean column order for BI tools
    .select(
        "product_id",
        "product_name",
        "category",
        "window_start",
        "window_end",
        "purchase_count",
        "total_revenue_usd",
        "avg_order_value",
        "unique_buyers"
    )
)

order_query = (
    order_gold.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="5 minutes")
    .option("checkpointLocation", "/tmp/delta/gold/order_metrics/_checkpoint")
    .start("/tmp/delta/gold/order_metrics")
)

# ---------------------------------------------------------------------------
# TERMINATION HANDLING
# ---------------------------------------------------------------------------
# Same monitoring loop as bronze and silver.
# awaitAnyTermination() unblocks as soon as any one stream dies, leaving
# the other running unmonitored with no error logged.

print("[INFO] Both gold streams running. Monitoring...")

while True:
    time.sleep(30)

    if not user_query.isActive:
        print("[ERROR] user_metrics gold stream died unexpectedly.")
        print(f"[ERROR] Last exception: {user_query.exception()}")
        order_query.stop()
        break

    if not order_query.isActive:
        print("[ERROR] order_metrics gold stream died unexpectedly.")
        print(f"[ERROR] Last exception: {order_query.exception()}")
        user_query.stop()
        break

    u_progress = user_query.lastProgress
    o_progress = order_query.lastProgress

    u_rows = u_progress["numInputRows"] if u_progress else 0
    o_rows = o_progress["numInputRows"] if o_progress else 0

    print(f"[PROGRESS] user_metrics: {u_rows} rows in last batch | "
          f"order_metrics: {o_rows} rows in last batch")

print("[INFO] Gold streaming job terminated.")