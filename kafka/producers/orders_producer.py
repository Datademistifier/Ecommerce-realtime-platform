"""
orders_producer.py
------------------
Simulates a live e-commerce order stream.
Produces JSON order events to the Kafka topic 'orders'.

Each event represents a customer placing an order and contains:
- order_id, customer_id, product_id, quantity, unit_price
- order_total (computed), order_status, shipping_state, timestamp

Run: python kafka/producers/orders_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
import os

# ── Config ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "orders"
EVENTS_PER_SECOND = 2          # production rate
TOTAL_EVENTS = None             # None = run forever

# ── Realistic sample data ─────────────────────────────────────
PRODUCTS = [
    {"product_id": "P001", "product_name": "Wireless Headphones",  "category": "Electronics", "base_price": 89.99},
    {"product_id": "P002", "product_name": "Running Shoes",        "category": "Footwear",    "base_price": 124.99},
    {"product_id": "P003", "product_name": "Coffee Maker",         "category": "Appliances",  "base_price": 59.99},
    {"product_id": "P004", "product_name": "Yoga Mat",             "category": "Fitness",     "base_price": 34.99},
    {"product_id": "P005", "product_name": "Desk Lamp",            "category": "Home Office", "base_price": 44.99},
    {"product_id": "P006", "product_name": "Water Bottle",         "category": "Fitness",     "base_price": 24.99},
    {"product_id": "P007", "product_name": "Laptop Stand",         "category": "Home Office", "base_price": 39.99},
    {"product_id": "P008", "product_name": "Bluetooth Speaker",    "category": "Electronics", "base_price": 69.99},
    {"product_id": "P009", "product_name": "Kitchen Knife Set",    "category": "Appliances",  "base_price": 79.99},
    {"product_id": "P010", "product_name": "Sunglasses",           "category": "Accessories", "base_price": 54.99},
]

STATES = ["TX", "CA", "NY", "FL", "WA", "IL", "OH", "GA", "NC", "AZ"]
STATUSES = ["PLACED", "PLACED", "PLACED", "PROCESSING", "SHIPPED"]  # weighted toward PLACED
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay"]


def generate_order_event() -> dict:
    """Generate a single realistic order event."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)
    # Add slight price variation (discounts, promotions)
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.05), 2)
    order_total = round(unit_price * quantity, 2)

    return {
        "event_type":      "ORDER_PLACED",
        "order_id":        f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "customer_id":     f"CUST-{random.randint(1000, 9999)}",
        "product_id":      product["product_id"],
        "product_name":    product["product_name"],
        "category":        product["category"],
        "quantity":        quantity,
        "unit_price":      unit_price,
        "order_total":     order_total,
        "order_status":    random.choice(STATUSES),
        "shipping_state":  random.choice(STATES),
        "payment_method":  random.choice(PAYMENT_METHODS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Reliability settings
        acks="all",               # wait for all replicas to acknowledge
        retries=3,
        linger_ms=5,              # small batching window for throughput
    )

    print(f"[orders_producer] Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[orders_producer] Publishing to topic: {TOPIC}")
    print(f"[orders_producer] Rate: {EVENTS_PER_SECOND} events/sec  |  Press Ctrl+C to stop\n")

    count = 0
    try:
        while TOTAL_EVENTS is None or count < TOTAL_EVENTS:
            event = generate_order_event()

            # Use customer_id as partition key — ensures all orders
            # from the same customer go to the same partition (ordering guarantee)
            producer.send(
                TOPIC,
                key=event["customer_id"].encode("utf-8"),
                value=event,
            )

            count += 1
            if count % 10 == 0:
                print(f"[orders_producer] Sent {count} events | "
                      f"Latest: {event['order_id']} | "
                      f"Total: ${event['order_total']}")
                producer.flush()

            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\n[orders_producer] Stopped. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
