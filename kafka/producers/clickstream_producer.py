"""
clickstream_producer.py
-----------------------
Simulates user browsing events on an e-commerce site.
Produces JSON events to the Kafka topic 'clickstream'.

Event types: PAGE_VIEW, PRODUCT_VIEW, ADD_TO_CART, REMOVE_FROM_CART,
             CHECKOUT_START, CHECKOUT_COMPLETE, SEARCH

These events form user sessions and enable funnel analysis,
session reconstruction, and conversion rate calculations in dbt.

Run: python kafka/producers/clickstream_producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "clickstream"
EVENTS_PER_SECOND = 8   # clicks arrive faster than orders

# Weighted event types — realistic funnel shape
EVENT_TYPES = [
    "PAGE_VIEW",         "PAGE_VIEW",         "PAGE_VIEW",
    "PRODUCT_VIEW",      "PRODUCT_VIEW",      "PRODUCT_VIEW",
    "SEARCH",            "SEARCH",
    "ADD_TO_CART",       "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "CHECKOUT_START",
    "CHECKOUT_COMPLETE",
]

PAGES = [
    "/", "/category/electronics", "/category/footwear",
    "/category/fitness", "/category/home-office",
    "/product/P001", "/product/P002", "/product/P003",
    "/product/P004", "/product/P005", "/product/P006",
    "/cart", "/checkout", "/search",
]

DEVICES = ["desktop", "desktop", "mobile", "mobile", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
REFERRERS = ["google", "direct", "email_campaign", "instagram", "facebook", "none"]

PRODUCT_IDS = [f"P{str(i).zfill(3)}" for i in range(1, 11)]
SEARCH_TERMS = [
    "headphones", "running shoes", "coffee maker", "yoga mat",
    "desk lamp", "speaker", "laptop stand", "sunglasses", "knife set",
]


def generate_click_event(session_id: str, customer_id: str) -> dict:
    event_type = random.choice(EVENT_TYPES)

    event = {
        "event_type":       event_type,
        "event_id":         f"EVT-{uuid.uuid4().hex[:8].upper()}",
        "session_id":       session_id,
        "customer_id":      customer_id,
        "page":             random.choice(PAGES),
        "device_type":      random.choice(DEVICES),
        "browser":          random.choice(BROWSERS),
        "referrer":         random.choice(REFERRERS),
        "event_timestamp":  datetime.now(timezone.utc).isoformat(),
    }

    # Enrich based on event type
    if event_type in ("PRODUCT_VIEW", "ADD_TO_CART", "REMOVE_FROM_CART"):
        event["product_id"] = random.choice(PRODUCT_IDS)
        event["page"] = f"/product/{event['product_id']}"

    if event_type == "SEARCH":
        event["search_term"] = random.choice(SEARCH_TERMS)
        event["results_count"] = random.randint(0, 48)

    if event_type in ("CHECKOUT_START", "CHECKOUT_COMPLETE"):
        event["cart_value"] = round(random.uniform(20, 400), 2)

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print(f"[clickstream_producer] Publishing to topic: {TOPIC} @ {EVENTS_PER_SECOND} events/sec")

    count = 0
    # Create a pool of active sessions (realistic: multiple users browsing simultaneously)
    sessions = [
        {"session_id": f"SES-{uuid.uuid4().hex[:8]}", "customer_id": f"CUST-{random.randint(1000,9999)}"}
        for _ in range(20)
    ]

    try:
        while True:
            session = random.choice(sessions)
            event = generate_click_event(session["session_id"], session["customer_id"])

            # Occasionally start a new session (user closes and reopens browser)
            if random.random() < 0.02:
                idx = sessions.index(session)
                sessions[idx] = {
                    "session_id":  f"SES-{uuid.uuid4().hex[:8]}",
                    "customer_id": f"CUST-{random.randint(1000, 9999)}"
                }

            producer.send(
                TOPIC,
                key=event["session_id"].encode("utf-8"),
                value=event,
            )
            count += 1

            if count % 50 == 0:
                print(f"[clickstream_producer] Sent {count} events | "
                      f"Latest: {event['event_type']} on {event['page']}")
                producer.flush()

            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        print(f"\n[clickstream_producer] Stopped. Total events: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
