"""Simulates e-commerce events: clicks, orders, inventory updates."""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------- Reference data (the "master" data of our shop) ----------
CATEGORIES = {
    "Electronics": ["Headphones", "Charger", "Speaker", "Webcam"],
    "Clothing": ["T-Shirt", "Jacket", "Jeans", "Sneakers"],
    "Home": ["Lamp", "Cushion", "Mug", "Blender"],
    "Books": ["Novel", "Cookbook", "Biography", "Textbook"],
}
WAREHOUSES = ["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST"]
PAYMENT_METHODS = ["card", "upi", "paypal", "cash_on_delivery"]


def build_products(n=50):
    products = []
    for i in range(1, n + 1):
        category = random.choice(list(CATEGORIES))
        noun = random.choice(CATEGORIES[category])
        products.append({
            "product_id": f"P{i:04d}",
            "product_name": f"{fake.color_name()} {noun}",
            "category": category,
            "unit_price": round(random.uniform(5, 300), 2),
        })
    return products


def build_customers(n=200):
    return [
        {
            "customer_id": f"C{i:04d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date="-2y").isoformat(),
        }
        for i in range(1, n + 1)
    ]


PRODUCTS = build_products()
CUSTOMERS = build_customers()


# ---------- Event builders ----------
def envelope(event_type, payload):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def make_click():
    product = random.choice(PRODUCTS)
    action = random.choices(
        ["page_view", "product_click", "add_to_cart"], weights=[60, 30, 10]
    )[0]
    return envelope("click", {
        "action": action,
        "customer_id": random.choice(CUSTOMERS)["customer_id"],
        "product_id": product["product_id"],
        "session_id": str(uuid.uuid4()),
        "device": random.choice(["mobile", "desktop", "tablet"]),
    })


def make_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    return envelope("order", {
        "order_id": f"O{uuid.uuid4().hex[:10].upper()}",
        "customer_id": random.choice(CUSTOMERS)["customer_id"],
        "product_id": product["product_id"],
        "quantity": qty,
        "unit_price": product["unit_price"],
        "total_amount": round(product["unit_price"] * qty, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choices(
            ["completed", "cancelled"], weights=[95, 5]
        )[0],
    })


def make_inventory_update():
    reason = random.choice(["restock", "sale", "return", "adjustment"])
    change = {
        "restock": random.randint(20, 200),
        "sale": -random.randint(1, 3),
        "return": 1,
        "adjustment": random.randint(-5, 5),
    }[reason]
    return envelope("inventory_update", {
        "product_id": random.choice(PRODUCTS)["product_id"],
        "warehouse": random.choice(WAREHOUSES),
        "quantity_change": change,
        "reason": reason,
    })


def generate_event():
    builder = random.choices(
        [make_click, make_order, make_inventory_update], weights=[70, 20, 10]
    )[0]
    return builder()


# ---------- Main loop ----------
def save_catalogs(folder="data"):
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/products.json", "w") as f:
        json.dump(PRODUCTS, f, indent=2)
    with open(f"{folder}/customers.json", "w") as f:
        json.dump(CUSTOMERS, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0, help="events per second")
    parser.add_argument("--max-events", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    save_catalogs()
    count = 0
    try:
        while args.max_events == 0 or count < args.max_events:
            event = generate_event()
            print(json.dumps(event), flush=True)
            # ~1% of the time, "deliver" the same event twice (realistic mess)
            if random.random() < 0.01:
                print(json.dumps(event), flush=True)
            count += 1
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()