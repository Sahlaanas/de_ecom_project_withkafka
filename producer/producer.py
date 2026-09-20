"""
Simulates a live e-commerce event stream: page views, purchases, and inventory
updates. Publishes each event as JSON to a Kafka topic.

Run:
    python producer/producer.py
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

fake = Faker()

# A fixed product catalog so purchases/inventory updates reference consistent products
PRODUCT_CATALOG = [
    {
        "product_id": f"P{str(i).zfill(4)}",
        "product_name": fake.unique.catch_phrase(),
        "category": random.choice(
            ["Electronics", "Home & Kitchen", "Apparel", "Sports", "Books", "Toys"]
        ),
        "price": round(random.uniform(5, 500), 2),
    }
    for i in range(1, 201)
]

# A pool of "known" customers so events reference consistent customer_ids
CUSTOMER_POOL = [
    {
        "customer_id": f"C{str(i).zfill(5)}",
        "email": fake.unique.email(),
        "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
        "country": fake.country_code(),
    }
    for i in range(1, 5001)
]


def make_page_view():
    product = random.choice(PRODUCT_CATALOG)
    customer = random.choice(CUSTOMER_POOL)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "page_view",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer["customer_id"],
        "product_id": product["product_id"],
        "session_id": str(uuid.uuid4()),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "referrer": random.choice(["google", "direct", "email", "social", "affiliate"]),
    }


def make_purchase():
    product = random.choice(PRODUCT_CATALOG)
    customer = random.choice(CUSTOMER_POOL)
    quantity = random.randint(1, 4)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "customer_email": customer["email"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total_amount": round(product["price"] * quantity, 2),
        "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "gift_card"]),
        "shipping_country": customer["country"],
    }


def make_inventory_update():
    product = random.choice(PRODUCT_CATALOG)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "inventory_update",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "warehouse_id": f"WH{random.randint(1, 6)}",
        "quantity_change": random.choice([-5, -3, -2, -1, 1, 10, 25, 50]),
        "reason": random.choice(["sale", "restock", "return", "damage", "adjustment"]),
    }


EVENT_GENERATORS = {
    "page_view": make_page_view,
    "purchase": make_purchase,
    "inventory_update": make_inventory_update,
}
# Realistic mix: mostly browsing, some purchases, occasional inventory movement
EVENT_WEIGHTS = {"page_view": 0.70, "purchase": 0.20, "inventory_update": 0.10}


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,
        retries=5,
    )


def main():
    producer = build_producer()
    print(f"[producer] streaming to {BOOTSTRAP_SERVERS} topic '{TOPIC}' "
          f"at ~{EVENTS_PER_SECOND} events/sec. Ctrl+C to stop.")
    sleep_time = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.2
    count = 0
    try:
        while True:
            event_type = random.choices(
                list(EVENT_WEIGHTS.keys()), weights=list(EVENT_WEIGHTS.values())
            )[0]
            event = EVENT_GENERATORS[event_type]()
            producer.send(TOPIC, key=event_type, value=event)
            count += 1
            if count % 50 == 0:
                print(f"[producer] sent {count} events (last: {event_type})")
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print(f"\n[producer] stopping. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
