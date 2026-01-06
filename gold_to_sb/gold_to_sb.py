import pandas as pd
from sqlalchemy import create_engine, text
import os


gold_folder = "/tmp/delta/gold"

DATABASE_URL = ""


engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})

if not os.path.exists(gold_folder):
    print(f"[ERROR] Gold folder does not exist: {gold_folder}")
    exit(1)

all_files = [os.path.join(gold_folder, f) for f in os.listdir(gold_folder) if f.endswith(".parquet")]
if not all_files:
    print("[INFO] No Parquet files found in Gold folder. Exiting.")
    exit(0)

df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)
print(f" Total rows read from Gold layer: {len(df)}")

if "record_id" not in df.columns or "event_date" not in df.columns:
    print("[ERROR] Gold data missing 'record_id' or 'event_date' column.")
    exit(1)

df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")


records = df.to_dict(orient="records")

with engine.begin() as conn:
    for record in records:
        conn.execute(
            text("""
                INSERT INTO gold_table (
                    record_id, product_id, product_name, category, brand,
                    event_date, total_events_count, click_count, purchase_count,
                    total_revenue_usd, purchase_revenue_usd, unique_users,
                    avg_revenue_per_event, conversion_rate, avg_price_per_purchase,
                    click_to_purchase_ratio, events_per_user, purchases_per_user
                )
                VALUES (
                    :record_id, :product_id, :product_name, :category, :brand,
                    :event_date, :total_events_count, :click_count, :purchase_count,
                    :total_revenue_usd, :purchase_revenue_usd, :unique_users,
                    :avg_revenue_per_event, :conversion_rate, :avg_price_per_purchase,
                    :click_to_purchase_ratio, :events_per_user, :purchases_per_user
                )
                ON CONFLICT (record_id) DO UPDATE SET
                    total_events_count = EXCLUDED.total_events_count,
                    click_count = EXCLUDED.click_count,
                    purchase_count = EXCLUDED.purchase_count,
                    total_revenue_usd = EXCLUDED.total_revenue_usd,
                    purchase_revenue_usd = EXCLUDED.purchase_revenue_usd,
                    unique_users = EXCLUDED.unique_users,
                    avg_revenue_per_event = EXCLUDED.avg_revenue_per_event,
                    conversion_rate = EXCLUDED.conversion_rate,
                    avg_price_per_purchase = EXCLUDED.avg_price_per_purchase,
                    click_to_purchase_ratio = EXCLUDED.click_to_purchase_ratio,
                    events_per_user = EXCLUDED.events_per_user,
                    purchases_per_user = EXCLUDED.purchases_per_user;
            """),
            record
        )

print(" Supabase upsert completed — all data synced successfully!")
