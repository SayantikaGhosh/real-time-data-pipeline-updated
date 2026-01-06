from confluent_kafka import Producer
from faker import Faker
import json
import time
import random
import socket
import uuid
from datetime import datetime, timedelta

fake = Faker()

# --- Wait for Kafka to be ready ---
def wait_for_kafka(host, port, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[WAIT] ✅ Kafka is available at {host}:{port}")
                return True
        except OSError:
            print(f"[WAIT] Waiting for Kafka at {host}:{port}...")
            time.sleep(2)
    raise TimeoutError(f"Kafka not available at {host}:{port} after {timeout} seconds")

wait_for_kafka("kafka", 9092)

# --- Kafka Configuration ---
conf = {'bootstrap.servers': 'kafka:9092'}
p = Producer(conf)

# --- Delivery report callback ---
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

# --- Product catalog ---
products = [
    {"product_id": "P001", "name": "Laptop", "category": "Electronics", "brand": "BrandA", "price": 1200.0},
    {"product_id": "P002", "name": "Headphones", "category": "Electronics", "brand": "BrandB", "price": 150.0},
    {"product_id": "P003", "name": "Shoes", "category": "Fashion", "brand": "BrandC", "price": 80.0},
    {"product_id": "P004", "name": "Watch", "category": "Accessories", "brand": "BrandD", "price": 200.0},
    {"product_id": "P005", "name": "Book", "category": "Education", "brand": "BrandE", "price": 25.0},
    {"product_id": "P006", "name": "Smartphone", "category": "Electronics", "brand": "BrandF", "price": 900.0},
    {"product_id": "P007", "name": "Tablet", "category": "Electronics", "brand": "BrandG", "price": 450.0},
    {"product_id": "P008", "name": "Backpack", "category": "Fashion", "brand": "BrandH", "price": 60.0},
    {"product_id": "P009", "name": "Sunglasses", "category": "Accessories", "brand": "BrandI", "price": 120.0},
    {"product_id": "P010", "name": "Desk Lamp", "category": "Home", "brand": "BrandJ", "price": 35.0},
]

# --- Fixed pool of users ---
users = [{"user_id": str(uuid.uuid4()), "user_name": fake.name()} for _ in range(50)]

# --- Generate realistic event (Jan 2023 → Jan 2025) ---
def generate_fake_event():
    product = random.choice(products)
    user = random.choice(users)

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 1, 31)
    delta_days = (end_date - start_date).days

    random_date = start_date + timedelta(
        days=random.randint(0, delta_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "user_name": user["user_name"],
        "event_type": random.choices(["click", "purchase"], weights=[70, 30])[0],
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "brand": product["brand"],
        "price": round(product["price"] * random.uniform(0.95, 1.05), 2),
        "timestamp": random_date.isoformat()
    }

NUM_EVENTS = 50  
print(f"[INFO]  Producing {NUM_EVENTS} events from Jan 2023 → Jan 2025...")
for _ in range(NUM_EVENTS):
    event = generate_fake_event()
    message = json.dumps(event)
    p.produce('test-topic', message.encode('utf-8'), callback=delivery_report)
    p.poll(0)
    time.sleep(0.2)  
p.flush()
print("[INFO]  All events produced successfully!")
