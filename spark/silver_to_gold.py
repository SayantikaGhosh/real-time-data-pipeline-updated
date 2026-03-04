from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    when,
    approx_count_distinct,
    lit,
    window
)

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df_user = spark.readStream.format("delta").load("/tmp/delta/silver/user_activity")
df_orders = spark.readStream.format("delta").load("/tmp/delta/silver/order_events")

# Smaller watermark for demo
df_user = df_user.withWatermark("event_time", "2 minutes")
df_orders = df_orders.withWatermark("event_time", "2 minutes")

# ================= USER METRICS =================
user_gold = (
    df_user
    .groupBy(
        "product_id",
        "category",
        window(col("event_time"), "1 minute")
    )
    .agg(
        spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        spark_sum(when(col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
        spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        approx_count_distinct("user_id").alias("unique_users")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
    # ✅ ADD CTR HERE
    .withColumn(
        "click_through_rate",
        when(col("view_count") > 0,
             col("click_count") / col("view_count")
        ).otherwise(lit(0.0))
    )
)

user_query = (
    user_gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/delta/gold/user_metrics/_checkpoint_v4")
    .start("/tmp/delta/gold/user_metrics")
)

# ================= ORDER METRICS =================
order_gold = (
    df_orders
    .groupBy(
        "product_id",
        "category",
        window(col("event_time"), "1 minute")
    )
    .agg(
        count("*").alias("purchase_count"),
        spark_sum("price").alias("total_revenue_usd"),
        approx_count_distinct("user_id").alias("unique_buyers")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window")
)

order_query = (
    order_gold.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/delta/gold/order_metrics/_checkpoint_v4")
    .start("/tmp/delta/gold/order_metrics")
)

spark.streams.awaitAnyTermination()