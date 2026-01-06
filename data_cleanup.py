from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# Create Spark session with Delta support
spark = SparkSession.builder \
    .appName("DeltaCleanup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Paths to your Delta tables
layers = {
    "bronze": "/tmp/delta/bronze",
    "silver": "/tmp/delta/silver",
    "gold": "/tmp/delta/gold"
}

# Loop through each layer and clean old versions
for name, path in layers.items():
    print(f"\n🧹 Cleaning Delta table: {name}")
    delta_table = DeltaTable.forPath(spark, path)
    
    # Show version history (just for verification)
    history_df = delta_table.history()
    count_versions = history_df.count()
    print(f"📜 Total versions in {name}: {count_versions}")
    
    # If more than 2 versions exist, clean up
    if count_versions > 2:
        # Set retention to 0 hours (override 7-day restriction)
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
        
        # Run vacuum to delete unreferenced files
        delta_table.vacuum(0)
        print(f"✅ Old versions of {name} cleaned, only latest data retained.")
    else:
        print(f"ℹ️ {name} has only {count_versions} versions, no cleanup needed.")

spark.stop()
