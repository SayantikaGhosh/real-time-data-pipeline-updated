from confluent_kafka import Producer
import json
import random
import socket
import uuid
import os
import tempfile
from datetime import datetime, timedelta, timezone
import time

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

KAFKA_HOST            = "kafka"
KAFKA_PORT            = 9092
KAFKA_WAIT_TIMEOUT    = 60       # seconds to wait for Kafka to be ready

RUN_DURATION_SECONDS  = 1200      # 20 minutes — long enough for Spark to build
                                  # meaningful windows and Gold to aggregate properly

NUM_USERS             = 100       # more users = more realistic traffic volume
MAX_CART_SIZE         = 6        # cart is cleared (cart abandonment) once it hits this
CART_ABANDON_CHANCE   = 0.15     # 15% chance per loop to randomly drop one cart item

# ---------------------------------------------------------------------------
# WAIT FOR KAFKA
# ---------------------------------------------------------------------------
# Tries to open a TCP connection to Kafka every 2 seconds.
# If Kafka isn't up within KAFKA_WAIT_TIMEOUT seconds, raises TimeoutError.

def wait_for_kafka(host, port, timeout=KAFKA_WAIT_TIMEOUT):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[WAIT] Kafka is ready at {host}:{port}")
                return
        except OSError:
            print("[WAIT] Kafka not ready yet, retrying in 2s...")
            time.sleep(2)
    raise TimeoutError(f"[ERROR] Kafka at {host}:{port} not available after {timeout}s")

wait_for_kafka(KAFKA_HOST, KAFKA_PORT)

# ---------------------------------------------------------------------------
# PRODUCER CONFIG
# ---------------------------------------------------------------------------
# acks=all         → leader + all in-sync replicas must confirm before success
# retries=5        → retry up to 5 times on transient failures
# idempotence=True → guarantees exactly-once delivery even if retries happen
# linger.ms=200    → wait up to 200ms to batch messages before sending.
#                    This is the key fix for small files: instead of sending
#                    one message at a time, the producer collects messages
#                    for 200ms and sends them as one batch to Kafka.
#                    Spark then sees one healthy batch instead of hundreds of
#                    tiny trickles, which means fewer, larger parquet files.
# batch.size=65536 → max batch size in bytes (64KB). Works together with
#                    linger.ms to control how much data is bundled per send.

conf = {
    "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "acks": "all",
    "retries": 5,
    "enable.idempotence": True,
    "linger.ms": 200,
    "batch.size": 65536,
}

producer = Producer(conf)

# ---------------------------------------------------------------------------
# PRODUCTS CATALOG
# 20 products across 5 categories. More variety means richer Gold aggregations.
# ---------------------------------------------------------------------------

products = [
    # Electronics
    {"product_id": "P001", "name": "Laptop",           "category": "Electronics",  "price": 1200.0},
    {"product_id": "P002", "name": "Smartphone",       "category": "Electronics",  "price": 900.0},
    {"product_id": "P003", "name": "Headphones",       "category": "Electronics",  "price": 150.0},
    {"product_id": "P004", "name": "Tablet",           "category": "Electronics",  "price": 600.0},
    {"product_id": "P005", "name": "Gaming Mouse",     "category": "Electronics",  "price": 110.0},
    {"product_id": "P006", "name": "Keyboard",         "category": "Electronics",  "price": 70.0},
    {"product_id": "P007", "name": "Monitor",          "category": "Electronics",  "price": 350.0},
    {"product_id": "P008", "name": "Webcam",           "category": "Electronics",  "price": 90.0},

    # Fashion
    {"product_id": "P009", "name": "Shoes",            "category": "Fashion",      "price": 80.0},
    {"product_id": "P010", "name": "Jacket",           "category": "Fashion",      "price": 150.0},
    {"product_id": "P011", "name": "Backpack",         "category": "Fashion",      "price": 50.0},
    {"product_id": "P012", "name": "Jeans",            "category": "Fashion",      "price": 60.0},
    {"product_id": "P013", "name": "Sneakers",         "category": "Fashion",      "price": 95.0},
    {"product_id": "P014", "name": "Hoodie",           "category": "Fashion",      "price": 55.0},

    # Accessories
    {"product_id": "P015", "name": "Watch",            "category": "Accessories",  "price": 200.0},
    {"product_id": "P016", "name": "Sunglasses",       "category": "Accessories",  "price": 120.0},
    {"product_id": "P017", "name": "Wallet",           "category": "Accessories",  "price": 60.0},
    {"product_id": "P018", "name": "Belt",             "category": "Accessories",  "price": 35.0},
    {"product_id": "P019", "name": "Cap",              "category": "Accessories",  "price": 25.0},

    # Education
    {"product_id": "P020", "name": "Book",             "category": "Education",    "price": 25.0},
    {"product_id": "P021", "name": "Notebook",         "category": "Education",    "price": 15.0},
    {"product_id": "P022", "name": "Pen Set",          "category": "Education",    "price": 12.0},
    {"product_id": "P023", "name": "Scientific Calc",  "category": "Education",    "price": 45.0},

    # Home
    {"product_id": "P024", "name": "Desk Lamp",        "category": "Home",         "price": 45.0},
    {"product_id": "P025", "name": "Coffee Mug",       "category": "Home",         "price": 18.0},
    {"product_id": "P026", "name": "Throw Pillow",     "category": "Home",         "price": 30.0},
    {"product_id": "P027", "name": "Wall Clock",       "category": "Home",         "price": 40.0},

    # Sports
    {"product_id": "P028", "name": "Yoga Mat",         "category": "Sports",       "price": 35.0},
    {"product_id": "P029", "name": "Dumbbells",        "category": "Sports",       "price": 60.0},
    {"product_id": "P030", "name": "Water Bottle",     "category": "Sports",       "price": 20.0},
    {"product_id": "P031", "name": "Running Shoes",    "category": "Sports",       "price": 110.0},
    {"product_id": "P032", "name": "Resistance Band",  "category": "Sports",       "price": 15.0},

    # Beauty
    {"product_id": "P033", "name": "Face Wash",        "category": "Beauty",       "price": 22.0},
    {"product_id": "P034", "name": "Moisturizer",      "category": "Beauty",       "price": 35.0},
    {"product_id": "P035", "name": "Lip Balm",         "category": "Beauty",       "price": 10.0},
    {"product_id": "P036", "name": "Shampoo",          "category": "Beauty",       "price": 18.0},

    # Kitchen
    {"product_id": "P037", "name": "Coffee Beans",     "category": "Kitchen",      "price": 28.0},
    {"product_id": "P038", "name": "Blender",          "category": "Kitchen",      "price": 75.0},
    {"product_id": "P039", "name": "Air Fryer",        "category": "Kitchen",      "price": 120.0},
    {"product_id": "P040", "name": "Cutting Board",    "category": "Kitchen",      "price": 22.0},
]
# ---------------------------------------------------------------------------
# USER STATE — persistent across restarts
# ---------------------------------------------------------------------------
# We persist users to disk so that if the container restarts mid-run,
# we don't generate a completely new set of user IDs (which would break
# session continuity in the data).

STATE_FILE = "data/user_state.json"

def save_state_atomic(filepath, state):
    """
    Write state to a temp file first, then atomically rename it to the real path.
    This prevents a corrupt state file if the process is killed mid-write.
    """
    dir_name = os.path.dirname(filepath)
    os.makedirs(dir_name, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False) as tmp:
        json.dump(state, tmp, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())   # force OS to flush to disk before rename
        temp_name = tmp.name

    os.replace(temp_name, filepath)   # atomic on POSIX systems


def load_or_initialize_users(num_users=NUM_USERS):
    """
    Load existing users from disk, or create a fresh set if none exist.
    Each user has a cart (list of product_ids) and a cart_updated_at
    timestamp so we can simulate cart abandonment after a timeout.
    """
    if os.path.exists(STATE_FILE):
        print(f"[INFO] Loaded existing user state from {STATE_FILE}")
        with open(STATE_FILE, "r") as f:
            return json.load(f)

    print(f"[INFO] No state file found. Initializing {num_users} new users.")
    users = {}
    for _ in range(num_users):
        user_id = str(uuid.uuid4())
        users[user_id] = {
            "cart": [],
            "cart_updated_at": datetime.now(timezone.utc).isoformat()
        }

    save_state_atomic(STATE_FILE, users)
    return users


users = load_or_initialize_users()

# ---------------------------------------------------------------------------
# LATE EVENT SIMULATION
# ---------------------------------------------------------------------------
# Simulates real-world network delays or out-of-order event delivery.
# 10% of events arrive 30s late, 5% arrive 90s late.
# Max lateness is 90 seconds — safely within the silver watermark of 10 minutes
# AND within the gold watermark of 5 minutes.

def adjust_for_lateness(event_time):
    r = random.random()
    if r < 0.10:
        # 10% of events: 30 seconds late
        return event_time - timedelta(seconds=30)
    elif r < 0.15:
        # 5% of events: 90 seconds late — still within all watermarks
        return event_time - timedelta(seconds=90)
    return event_time   # 85% of events: on time

# ---------------------------------------------------------------------------
# CART HELPERS
# ---------------------------------------------------------------------------

def maybe_abandon_cart(user_state, user_id):
    """
    Simulates cart abandonment — a very real ecommerce behaviour.
    If the cart is at or above MAX_CART_SIZE, or with a 15% random chance,
    drop one random item from the cart.
    This prevents carts from filling up completely over time, which would
    cause add_to_cart events to silently stop firing.
    """
    cart = user_state[user_id]["cart"]
    if not cart:
        return False

    if len(cart) >= MAX_CART_SIZE or random.random() < CART_ABANDON_CHANCE:
        cart.pop(random.randrange(len(cart)))
        user_state[user_id]["cart_updated_at"] = datetime.now(timezone.utc).isoformat()
        return True

    return False

# ---------------------------------------------------------------------------
# SESSION GENERATOR
# ---------------------------------------------------------------------------
# Simulates one user browsing session. The funnel:
#   view (100%) → click (70%) → add_to_cart (60%) → purchase (40%)
# Returns a list of (event_type, event_time, product) tuples.

def generate_session(user_id, user_state):
    session_id    = str(uuid.uuid4())
    session_start = datetime.now(timezone.utc)
    product       = random.choice(products)

    events       = []
    current_time = session_start

    # VIEW — always happens
    events.append(("view", current_time, product))

    # CLICK — 70% chance
    if random.random() < 0.70:
        current_time += timedelta(seconds=random.randint(5, 20))
        events.append(("click", current_time, product))

        # ADD TO CART — 60% chance, only if not already in cart
        if random.random() < 0.60:
            current_time += timedelta(seconds=random.randint(5, 30))
            events.append(("add_to_cart", current_time, product))

            if product["product_id"] not in user_state[user_id]["cart"]:
                user_state[user_id]["cart"].append(product["product_id"])
                user_state[user_id]["cart_updated_at"] = datetime.now(timezone.utc).isoformat()

            # PURCHASE — 40% chance
            if random.random() < 0.40:
                current_time += timedelta(seconds=random.randint(10, 60))
                events.append(("purchase", current_time, product))

                if product["product_id"] in user_state[user_id]["cart"]:
                    user_state[user_id]["cart"].remove(product["product_id"])
                    user_state[user_id]["cart_updated_at"] = datetime.now(timezone.utc).isoformat()

    return session_id, events

# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------
# Each iteration:
#   1. Shuffles user order so users don't always fire in the same sequence
#   2. Adds a tiny stagger between users (1–5ms) — just enough to break
#      lockstep without eating into event volume. The previous 10–80ms stagger
#      multiplied across 50 users added 2.25s per loop, reducing total events
#      from ~18,000 to only ~834. Now: 1–5ms × 50 users = ~0.15s per loop.
#   3. Batches all produce() calls, then calls poll() ONCE per user
#   4. Only saves state to disk if something actually changed (dirty flag)
#   5. Sleeps a variable amount between 2–5 seconds to vary batch sizes

print(f"[INFO] Starting event generation — will run for {RUN_DURATION_SECONDS}s "
      f"with {len(users)} users and {len(products)} products.")

start_time    = time.time()
loop_count    = 0
total_events  = 0

while time.time() - start_time < RUN_DURATION_SECONDS:

    loop_count   += 1
    state_dirty   = False
    loop_events   = 0

    elapsed = time.time() - start_time
    if 480 <= elapsed <= 600:
        active_users = list(users.keys())
    else:
        active_users = random.sample(list(users.keys()), k=max(1, int(len(users) * 0.4)))

    random.shuffle(active_users)

    # Shuffle user order each loop — breaks the lockstep pattern


    for user_id in active_users:

        # Tiny stagger between users (1–5ms).
        # Just enough to break lockstep — not enough to kill event volume.
        # Previous value was 10–80ms which added 2.25s per loop across
        # 50 users and reduced total events from ~18,000 to only ~834.
        time.sleep(random.uniform(0.001, 0.003))

        # Maybe clear one cart item to prevent cart saturation
        if maybe_abandon_cart(users, user_id):
            state_dirty = True

        session_id, session_events = generate_session(user_id, users)

        for event_type, event_time, product in session_events:

            # Apply late arrival simulation
            event_time = adjust_for_lateness(event_time)

            # producer_time = when this app created and sent the event.
            # This is NOT the same as ingestion_time (when Kafka received it).
            # kafka_to_bronze.py captures kafka_timestamp from the broker,
            # which is the true ingestion time. Having both lets you measure
            # producer → Kafka lag in your observability dashboards.
            producer_time = datetime.now(timezone.utc).isoformat()

            event = {
                "event_id":      str(uuid.uuid4()),
                "session_id":    session_id,
                "user_id":       user_id,
                "event_type":    event_type,
                "product_id":    product["product_id"],
                "product_name":  product["name"],
                "category":      product["category"],
                "price":         product["price"],
                "event_time":    event_time.isoformat(),
                "producer_time": producer_time,
            }

            topic = "order_events" if event_type == "purchase" else "user_activity_events"

            producer.produce(
                topic=topic,
                key=user_id,
                value=json.dumps(event).encode("utf-8")
            )

            loop_events  += 1
            total_events += 1

            # Track cart changes for dirty flag
            if event_type in ("add_to_cart", "purchase"):
                state_dirty = True

        # poll() once per user, not once per message.
        # This lets the producer's internal buffer accumulate messages
        # across the whole user session before handing off to the network.
        # Combined with linger.ms=200 in the producer config, this means
        # Kafka receives proper batches → Spark sees proper batch sizes →
        # parquet files are a healthy size instead of a few KB each.
        producer.poll(0)

    # Only write state file if something actually changed this loop
    if state_dirty:
        save_state_atomic(STATE_FILE, users)

    elapsed = time.time() - start_time
    print(f"[LOOP {loop_count:03d}] +{loop_events} events | "
          f"total={total_events} | elapsed={elapsed:.0f}s")

    # Variable sleep between 1–2 seconds.
    # This varies the batch sizes Spark sees, which is more realistic
    # than a fixed 3s heartbeat every single time.
    time.sleep(random.uniform(1.0,2.0))

# Final flush — drains any messages still in the producer's internal buffer
producer.flush()

print(f"[INFO] Done. Sent {total_events} events across {loop_count} loops "
      f"in {time.time() - start_time:.0f}s.")