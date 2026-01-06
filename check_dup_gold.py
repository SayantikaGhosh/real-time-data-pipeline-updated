import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# ✅ Initialize Spark with Delta support
builder = (
    SparkSession.builder.appName("CheckGoldDuplicates")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ Load the latest Gold table snapshot
gold_path = "/tmp/delta/gold"
gold_df = spark.read.format("delta").load(gold_path)

# Convert to Pandas for easy duplicate check
pdf = gold_df.toPandas()

print(f"✅ Total rows loaded: {len(pdf)}")
print(f"📋 Columns in Gold layer: {list(pdf.columns)}")

# ✅ Check for duplicates based on 'record_id'
if 'record_id' in pdf.columns:
    dup_record_id = pdf[pdf.duplicated(subset=['record_id'], keep=False)]
    print(f"\n⚠️ Found {len(dup_record_id)} duplicate rows based on 'record_id'.")
    if not dup_record_id.empty:
        print("🔍 Example duplicates:")
        print(dup_record_id.head(10))
else:
    print("⚠️ Column 'record_id' not found in Gold layer.")

# ✅ Check for duplicates based on (product_id, event_date)
if {'product_id', 'event_date'}.issubset(pdf.columns):
    dup_combination = pdf[pdf.duplicated(subset=['product_id', 'event_date'], keep=False)]
    print(f"\n⚠️ Found {len(dup_combination)} duplicate rows based on (product_id, event_date).")
    if not dup_combination.empty:
        print("🔍 Example duplicates:")
        print(dup_combination.head(10))
else:
    print("⚠️ Columns 'product_id' or 'event_date' not found.")
