"""
inventory_producer.py
---------------------
Simulates real-time inventory updates — stock level changes
as orders are placed and restocking events occur.

Produces JSON events to Kafka topic 'inventory_updates'.

Run: python kafka/producers/inventory_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "inventory_updates"

PRODUCT_IDS   = [f"P{str(i).zfill(3)}" for i in range(1, 11)]
WAREHOUSES    = ["WH-TX-01", "WH-CA-01", "WH-NY-01", "WH-FL-01"]
UPDATE_TYPES  = ["SALE", "SALE", "SALE", "RESTOCK", "ADJUSTMENT"]

# Simulated current stock levels
stock = {p: random.randint(50, 500) for p in PRODUCT_IDS}


def generate_inventory_event() -> dict:
    product_id  = random.choice(PRODUCT_IDS)
    update_type = random.choice(UPDATE_TYPES)

    if update_type == "SALE":
        qty_change = -random.randint(1, 5)
    elif update_type == "RESTOCK":
        qty_change = random.randint(50, 200)
    else:  # ADJUSTMENT
        qty_change = random.randint(-10, 10)

    stock[product_id] = max(0, stock[product_id] + qty_change)

    return {
        "event_type":        "INVENTORY_UPDATE",
        "update_id":         f"INV-{uuid.uuid4().hex[:8].upper()}",
        "product_id":        product_id,
        "warehouse_id":      random.choice(WAREHOUSES),
        "update_type":       update_type,
        "quantity_change":   qty_change,
        "stock_after":       stock[product_id],
        "low_stock_alert":   stock[product_id] < 20,
        "event_timestamp":   datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print(f"[inventory_producer] Publishing to topic: {TOPIC}")
    count = 0

    try:
        while True:
            event = generate_inventory_event()
            producer.send(TOPIC, key=event["product_id"].encode("utf-8"), value=event)
            count += 1

            if count % 20 == 0:
                alerts = [p for p, s in stock.items() if s < 20]
                print(f"[inventory_producer] Sent {count} events | "
                      f"Low stock alerts: {alerts or 'none'}")
                producer.flush()

            time.sleep(0.5)

    except KeyboardInterrupt:
        print(f"\n[inventory_producer] Stopped. Total events: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
