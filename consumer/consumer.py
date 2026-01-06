from confluent_kafka import Consumer, KafkaException
import json
import time
import socket
import sys

def wait_for_kafka(host, port, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[INFO] Kafka is available at {host}:{port}", flush=True)
                return
        except OSError:
            print(f"[WAIT] Waiting for Kafka at {host}:{port}...", flush=True)
            time.sleep(2)
    print(f"[ERROR] Kafka not available at {host}:{port} after {timeout} seconds", flush=True)
    sys.exit(1)

# Step 1: Wait for Kafka to be ready
wait_for_kafka("kafka", 9092)

# Step 2: Kafka Consumer configuration
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

print("[STARTED] Kafka consumer started. Waiting for messages...", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[ERROR] {msg.error()}", flush=True)
            continue

        # Deserialize JSON message
        try:
            data = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError:
            print("[ERROR] Failed to decode JSON", flush=True)
            continue

        # Print nicely formatted event including new product fields
        print(
            f"[RECEIVED] EventID: {data.get('event_id')} | "
            f"User: {data.get('user_name')} ({data.get('user_id')}) | "
            f"Type: {data.get('event_type').upper()} | "
            f"ProductID: {data.get('product_id')} | "
            f"Product: {data.get('product_name')} | "
            f"Category: {data.get('category')} | "
            f"Brand: {data.get('brand')} | "
            f"Price: ${data.get('price')} | "
            f"Timestamp: {data.get('timestamp')}",
            flush=True
        )

except KeyboardInterrupt:
    print("[STOP] Consumer interrupted.", flush=True)

finally:
    print("[CLOSE] Closing Kafka consumer.", flush=True)
    consumer.close()
