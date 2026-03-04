from confluent_kafka import Producer
import json
import random
import socket
import uuid
import os
import tempfile
from datetime import datetime, timedelta, timezone
import time

# This loop will run for 30 seconds. it tries to open a tcp connection at kafka:9092 and try for 2 sec, 
# if it fails then then it sleeps for 2 sec then try again till 30 sec ends 
# each loop takes about 4 sec , so 7 - 8 attempts and it returns ends 

def wait_for_kafka(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[WAIT] Kafka available at {host}:{port}")
                return
        except OSError:
            print("[WAIT] Waiting for Kafka...")
            time.sleep(2)
    raise TimeoutError("Kafka not available")

wait_for_kafka("kafka", 9092)


conf = {
    "bootstrap.servers": "kafka:9092", # kafka server address
    "acks": "all", # leader + all replicas must confirm
    "retries": 5, # tries for 5 times if msg not sent
    "enable.idempotence": True # no duplicate sent event if multiple tries happens
}

producer = Producer(conf)

# fixed number of product data
products = [
    {"product_id": "P001", "name": "Laptop", "category": "Electronics", "price": 1200.0},
    {"product_id": "P002", "name": "Headphones", "category": "Electronics", "price": 150.0},
    {"product_id": "P003", "name": "Shoes", "category": "Fashion", "price": 80.0},
    {"product_id": "P004", "name": "Watch", "category": "Accessories", "price": 200.0},
    {"product_id": "P005", "name": "Book", "category": "Education", "price": 25.0},

    {"product_id": "P006", "name": "Tablet", "category": "Electronics", "price": 600.0},
    {"product_id": "P007", "name": "Keyboard", "category": "Electronics", "price": 70.0},
    {"product_id": "P008", "name": "Backpack", "category": "Fashion", "price": 50.0},
    {"product_id": "P009", "name": "Sunglasses", "category": "Accessories", "price": 120.0},
    {"product_id": "P010", "name": "Notebook", "category": "Education", "price": 15.0},

    {"product_id": "P011", "name": "Smartphone", "category": "Electronics", "price": 900.0},
    {"product_id": "P012", "name": "Jacket", "category": "Fashion", "price": 150.0},
    {"product_id": "P013", "name": "Wallet", "category": "Accessories", "price": 60.0},
    {"product_id": "P014", "name": "Pen Set", "category": "Education", "price": 35.0},
    {"product_id": "P015", "name": "Gaming Mouse", "category": "Electronics", "price": 110.0},
]

# persistent user and their state
STATE_FILE = "data/user_state.json"
# Start program → Load file → Modify cart → Save file
def save_state_atomic(filepath, state):
    dir_name = os.path.dirname(filepath)
    os.makedirs(dir_name, exist_ok=True)

    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False) as tmp:
        json.dump(state, tmp, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_name = tmp.name

    os.replace(temp_name, filepath)

# persistent users

def load_or_initialize_users():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)

    users = {}
    for _ in range(30):
        user_id = str(uuid.uuid4())
        users[user_id] = {
            "cart": []
        }

    save_state_atomic(STATE_FILE, users)
    return users

users = load_or_initialize_users()

# late events generation

def adjust_for_lateness(event_time):
    r = random.random()
    if r < 0.10:
        return event_time - timedelta(minutes=2)
    elif r < 0.15:
        return event_time - timedelta(minutes=4)
    return event_time


# Session Generator

def generate_session(user_id, user_state):
    session_id = str(uuid.uuid4())
    session_start = datetime.now(timezone.utc)
    product = random.choice(products)

    events = []
    current_time = session_start

    # VIEW
    events.append(("view", current_time, product))

    # CLICK (70%)
    if random.random() < 0.7:
        current_time += timedelta(seconds=random.randint(5, 20))
        events.append(("click", current_time, product))

        # ADD TO CART (60%)
        if random.random() < 0.6:
            current_time += timedelta(seconds=random.randint(5, 30))
            events.append(("add_to_cart", current_time, product))
            if product["product_id"] not in user_state[user_id]["cart"]:
                user_state[user_id]["cart"].append(product["product_id"])

            # PURCHASE (40%)
            if random.random() < 0.4:
                current_time += timedelta(seconds=random.randint(10, 60))
                events.append(("purchase", current_time, product))
                if product["product_id"] in user_state[user_id]["cart"]:
                    user_state[user_id]["cart"].remove(product["product_id"])

    return session_id, events

# ----------------------------
# Main Loop (One Session Per User)
# outer loop users, inner loop events of that user
# ----------------------------
print("[INFO] Starting continuous event generation...")

RUN_DURATION_SECONDS = 240   # 4 minutes
start_time = time.time()

while time.time() - start_time < RUN_DURATION_SECONDS:

    for user_id in users.keys():

        session_id, session_events = generate_session(user_id, users)

        for event_type, event_time, product in session_events:

            event_time = adjust_for_lateness(event_time)

            event = {
                "event_id": str(uuid.uuid4()),
                "session_id": session_id,
                "user_id": user_id,
                "event_type": event_type,
                "product_id": product["product_id"],
                "category": product["category"],
                "price": product["price"],
                "event_time": event_time.isoformat(),
                "ingestion_time": datetime.now(timezone.utc).isoformat()
            }

            topic = "order_events" if event_type == "purchase" else "user_activity_events"

            producer.produce(
                topic=topic,
                key=user_id,
                value=json.dumps(event).encode("utf-8")
            )

            producer.poll(0)

    save_state_atomic(STATE_FILE, users)

    time.sleep(3)   # small pause to reduce CPU pressure

producer.flush()
print("[INFO] Continuous event generation completed.")