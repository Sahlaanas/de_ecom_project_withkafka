"""Publishes simulated e-commerce events to Kafka."""
import argparse
import json
import random
import time

from confluent_kafka import Producer

from generate_events import generate_event

TOPIC = "ecommerce-events"


def delivery_report(err, msg):
    """Called once per message to report success or failure."""
    if err is not None:
        print(f"Delivery FAILED: {err}")


def message_key(event):
    """Same customer (or product) -> same partition -> ordered events."""
    payload = event["payload"]
    return payload.get("customer_id") or payload["product_id"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=2.0, help="events per second")
    parser.add_argument("--max-events", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
    })

    count = 0
    try:
        while args.max_events == 0 or count < args.max_events:
            event = generate_event()
            # ~1% of events get delivered twice, like real-world at-least-once systems
            copies = 2 if random.random() < 0.01 else 1
            for _ in range(copies):
                producer.produce(
                    TOPIC,
                    key=message_key(event),
                    value=json.dumps(event),
                    on_delivery=delivery_report,
                )
            producer.poll(0)  # lets the client run delivery callbacks
            count += 1
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush()  # wait for any buffered messages to be sent
        print(f"Sent {count} events")


if __name__ == "__main__":
    main()