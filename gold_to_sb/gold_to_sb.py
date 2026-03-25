import pandas as pd
from sqlalchemy import create_engine, text
import os
import time

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

USER_GOLD_PATH  = "/tmp/delta/gold/user_metrics"
ORDER_GOLD_PATH = "/tmp/delta/gold/order_metrics"

# Columns to sync — explicitly defined so extra parquet columns never
# accidentally get passed to SQL and cause a parameter binding error.
USER_METRICS_COLS = [
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
    "add_to_cart_rate",
]

ORDER_METRICS_COLS = [
    "product_id",
    "product_name",
    "category",
    "window_start",
    "window_end",
    "purchase_count",
    "total_revenue_usd",
    "avg_order_value",
    "unique_buyers",
]

# ---------------------------------------------------------------------------
# SUPABASE CONNECTION
# ---------------------------------------------------------------------------
# pool_pre_ping=True — verifies the connection is alive before using it.
# Prevents "connection closed" errors when Supabase drops idle connections.

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("[ERROR] DATABASE_URL environment variable is not set.")
    exit(1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"sslmode": "require"},
    pool_pre_ping=True
)

# ---------------------------------------------------------------------------
# RETRY HELPER
# ---------------------------------------------------------------------------
# Wraps any function with simple retry logic.
# If Supabase is briefly unavailable, retries up to max_retries times
# with exponential backoff before giving up.

def with_retry(fn, max_retries=3, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries:
                print(f"[ERROR] Failed after {max_retries} attempts: {e}")
                raise
            print(f"[WARN] Attempt {attempt} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
            delay *= 2   # exponential backoff

# ---------------------------------------------------------------------------
# LOAD PARQUET FILES
# ---------------------------------------------------------------------------
# Recursively walks the Delta table folder and loads all part-*.parquet files.
# Skips _delta_log and _checkpoint folders — those are metadata, not data.
# Returns a filtered DataFrame with only the columns we need for the sync,
# or None if no parquet files are found.

def load_parquet_folder(folder_path, columns):
    if not os.path.exists(folder_path):
        print(f"[ERROR] Folder does not exist: {folder_path}")
        return None

    parquet_files = []

    for root, dirs, files in os.walk(folder_path):
        # Skip Delta metadata and Spark checkpoint folders
        if "_delta_log" in root or "_checkpoint" in root:
            continue
        for file in files:
            if file.startswith("part-") and file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))

    if not parquet_files:
        print(f"[INFO] No parquet files found in {folder_path}")
        return None

    print(f"[DEBUG] Found {len(parquet_files)} parquet file(s) in {folder_path}:")
    for f in parquet_files:
        print(f"        {f}")

    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True
    )

    # Only keep the columns we're going to insert — prevents extra columns
    # in the parquet file from causing SQL parameter binding errors
    existing_cols = [c for c in columns if c in df.columns]
    missing_cols  = [c for c in columns if c not in df.columns]

    if missing_cols:
        print(f"[WARN] Missing columns in parquet: {missing_cols}")

    df = df[existing_cols]

    # Convert timestamps
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
    if "window_end" in df.columns:
        df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")

    # Drop rows where window_start is null — can't upsert without the conflict key
    before = len(df)
    df = df.dropna(subset=["window_start"])
    dropped = before - len(df)
    if dropped > 0:
        print(f"[WARN] Dropped {dropped} rows with null window_start")

    print(f"[INFO] Loaded {len(df)} rows from {folder_path}")
    return df


# ---------------------------------------------------------------------------
# BULK UPSERT HELPER
# ---------------------------------------------------------------------------
# Uses executemany() to send all rows in a single round trip instead of
# one SQL call per row. For 1000 rows this reduces network calls from
# 1000 to 1 — dramatically faster especially over a remote Supabase connection.

def bulk_upsert(conn, sql, records):
    if not records:
        return
    conn.execute(text(sql), records)


# ---------------------------------------------------------------------------
# SYNC USER METRICS
# ---------------------------------------------------------------------------

print("[INFO] Starting Supabase sync...")

user_df = load_parquet_folder(USER_GOLD_PATH, USER_METRICS_COLS)

if user_df is not None and len(user_df) > 0:

    records = user_df.to_dict(orient="records")

    upsert_sql = """
        INSERT INTO user_metrics (
            product_id,
            product_name,
            category,
            window_start,
            window_end,
            view_count,
            click_count,
            add_to_cart_count,
            unique_users,
            click_through_rate,
            add_to_cart_rate
        )
        VALUES (
            :product_id,
            :product_name,
            :category,
            :window_start,
            :window_end,
            :view_count,
            :click_count,
            :add_to_cart_count,
            :unique_users,
            :click_through_rate,
            :add_to_cart_rate
        )
        ON CONFLICT (product_id, window_start)
        DO UPDATE SET
            product_name      = EXCLUDED.product_name,
            view_count        = EXCLUDED.view_count,
            click_count       = EXCLUDED.click_count,
            add_to_cart_count = EXCLUDED.add_to_cart_count,
            unique_users      = EXCLUDED.unique_users,
            click_through_rate = EXCLUDED.click_through_rate,
            add_to_cart_rate  = EXCLUDED.add_to_cart_rate;
    """

    def sync_user():
        with engine.begin() as conn:
            bulk_upsert(conn, upsert_sql, records)
        print(f"[INFO] User metrics synced — {len(records)} rows upserted.")

    with_retry(sync_user)

else:
    print("[INFO] No user metrics to sync.")


# ---------------------------------------------------------------------------
# SYNC ORDER METRICS
# ---------------------------------------------------------------------------

order_df = load_parquet_folder(ORDER_GOLD_PATH, ORDER_METRICS_COLS)

if order_df is not None and len(order_df) > 0:

    records = order_df.to_dict(orient="records")

    upsert_sql = """
        INSERT INTO order_metrics (
            product_id,
            product_name,
            category,
            window_start,
            window_end,
            purchase_count,
            total_revenue_usd,
            avg_order_value,
            unique_buyers
        )
        VALUES (
            :product_id,
            :product_name,
            :category,
            :window_start,
            :window_end,
            :purchase_count,
            :total_revenue_usd,
            :avg_order_value,
            :unique_buyers
        )
        ON CONFLICT (product_id, window_start)
        DO UPDATE SET
            product_name      = EXCLUDED.product_name,
            purchase_count    = EXCLUDED.purchase_count,
            total_revenue_usd = EXCLUDED.total_revenue_usd,
            avg_order_value   = EXCLUDED.avg_order_value,
            unique_buyers     = EXCLUDED.unique_buyers;
    """

    def sync_order():
        with engine.begin() as conn:
            bulk_upsert(conn, upsert_sql, records)
        print(f"[INFO] Order metrics synced — {len(records)} rows upserted.")

    with_retry(sync_order)

else:
    print("[INFO] No order metrics to sync.")


print("[INFO] Supabase sync completed successfully.")