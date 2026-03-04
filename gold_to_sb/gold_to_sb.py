import pandas as pd
from sqlalchemy import create_engine, text
import os

# -----------------------------------
# Paths to Gold Delta
# -----------------------------------
USER_GOLD_PATH = "/tmp/delta/gold/user_metrics"
ORDER_GOLD_PATH = "/tmp/delta/gold/order_metrics"

# -----------------------------------
# Supabase connection
# -----------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("[ERROR] DATABASE_URL not set.")
    exit(1)

engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})


# -----------------------------------
# Recursively load parquet files
# -----------------------------------
def load_parquet_folder(folder_path):
    if not os.path.exists(folder_path):
        print(f"[ERROR] Folder does not exist: {folder_path}")
        return None

    parquet_files = []

    for root, dirs, files in os.walk(folder_path):

        # 🔥 Skip Delta metadata folder
        if "_delta_log" in root:
            continue

        for file in files:
            # 🔥 Only include actual data files
            if file.startswith("part-") and file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))

    if not parquet_files:
        print(f"[INFO] No parquet files found in {folder_path}")
        return None

    print("[DEBUG] Data files detected:")
    for f in parquet_files:
        print("   ", f)

    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True
    )

    print(f"[INFO] Loaded {len(df)} rows from {folder_path}")
    return df


# ===================================
# 🔵 Sync USER METRICS
# ===================================
user_df = load_parquet_folder(USER_GOLD_PATH)

if user_df is not None:

    user_df["window_start"] = pd.to_datetime(user_df["window_start"], errors="coerce")
    user_df["window_end"] = pd.to_datetime(user_df["window_end"], errors="coerce")

    with engine.begin() as conn:
        for _, row in user_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO user_metrics (
                        product_id,
                        category,
                        window_start,
                        window_end,
                        view_count,
                        click_count,
                        add_to_cart_count,
                        unique_users,
                        click_through_rate
                    )
                    VALUES (
                        :product_id,
                        :category,
                        :window_start,
                        :window_end,
                        :view_count,
                        :click_count,
                        :add_to_cart_count,
                        :unique_users,
                        :click_through_rate
                    )
                    ON CONFLICT (product_id, window_start)
                    DO UPDATE SET
                        view_count = EXCLUDED.view_count,
                        click_count = EXCLUDED.click_count,
                        add_to_cart_count = EXCLUDED.add_to_cart_count,
                        unique_users = EXCLUDED.unique_users,
                        click_through_rate = EXCLUDED.click_through_rate;
                """),
                row.to_dict()
            )

    print(" User metrics synced successfully.")


# ===================================
# 🟡 Sync ORDER METRICS
# ===================================
order_df = load_parquet_folder(ORDER_GOLD_PATH)

if order_df is not None:

    order_df["window_start"] = pd.to_datetime(order_df["window_start"], errors="coerce")
    order_df["window_end"] = pd.to_datetime(order_df["window_end"], errors="coerce")

    with engine.begin() as conn:
        for _, row in order_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO order_metrics (
                        product_id,
                        category,
                        window_start,
                        window_end,
                        purchase_count,
                        total_revenue_usd,
                        unique_buyers
                    )
                    VALUES (
                        :product_id,
                        :category,
                        :window_start,
                        :window_end,
                        :purchase_count,
                        :total_revenue_usd,
                        :unique_buyers
                    )
                    ON CONFLICT (product_id, window_start)
                    DO UPDATE SET
                        purchase_count = EXCLUDED.purchase_count,
                        total_revenue_usd = EXCLUDED.total_revenue_usd,
                        unique_buyers = EXCLUDED.unique_buyers;
                """),
                row.to_dict()
            )

    print(" Order metrics synced successfully.")

print(" Supabase sync completed successfully.")