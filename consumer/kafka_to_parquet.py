"""Reads events from Kafka in micro-batches and writes partitioned Parquet files."""
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
import boto3
from dotenv import load_dotenv

load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "raw_events"
s3 = boto3.client("s3") if S3_BUCKET else None

TOPIC = "ecommerce-events"
LAKE_DIR = "data/lake/raw_events"
DEAD_LETTER_FILE = "data/dead_letter/bad_messages.jsonl"
BATCH_SIZE = 200        # write a file after this many events...
BATCH_SECONDS = 30      # ...or after this many seconds, whichever comes first
REQUIRED_FIELDS = {"event_id", "event_type", "event_timestamp", "payload"}


def parse_message(msg):
    """Turn a Kafka message into a flat row. Raises on bad data."""
    event = json.loads(msg.value().decode("utf-8"))
    if not isinstance(event, dict):
        raise ValueError("message is not a JSON object")
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"missing fields: {sorted(missing)}")
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_timestamp": event["event_timestamp"],
        "payload": json.dumps(event["payload"]),   # keep raw; parse later in the warehouse
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
    }


def send_to_dead_letter(msg, error):
    os.makedirs(os.path.dirname(DEAD_LETTER_FILE), exist_ok=True)
    raw = msg.value().decode("utf-8", errors="replace") if msg.value() else None
    record = {
        "partition": msg.partition(),
        "offset": msg.offset(),
        "error": str(error),
        "raw_value": raw,
    }
    with open(DEAD_LETTER_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def write_batch(rows):
    now = datetime.now(timezone.utc)
    folder = os.path.join(
        LAKE_DIR, f"ingest_date={now:%Y-%m-%d}", f"ingest_hour={now:%H}"
    )
    os.makedirs(folder, exist_ok=True)
    for row in rows:
        row["ingested_at"] = now.isoformat()

    table = pa.Table.from_pylist(rows)
    path = os.path.join(folder, f"batch_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}.parquet")
    tmp_path = path + ".tmp"
    pq.write_table(table, tmp_path, compression="snappy")
    os.replace(tmp_path, path)   # rename is atomic: readers never see half-written files
    return path


def upload_to_s3(local_path, attempts=3):
    """Same local path -> same S3 key, so a retry safely overwrites itself."""
    relative = os.path.relpath(local_path, LAKE_DIR).replace(os.sep, "/")
    key = f"{S3_PREFIX}/{relative}"
    for attempt in range(1, attempts + 1):
        try:
            s3.upload_file(local_path, S3_BUCKET, key)
            return key
        except Exception as e:
            print(f"Upload attempt {attempt} failed: {e}")
            if attempt == attempts:
                raise
            time.sleep(2 ** attempt)   # back off: 2s, then 4s


def flush_batch(consumer, rows):
    path = write_batch(rows)
    destination = path
    if s3:
        key = upload_to_s3(path)
        destination = f"s3://{S3_BUCKET}/{key}"
    consumer.commit(asynchronous=False)   # only after the file is safely stored
    print(f"Wrote {len(rows)} rows -> {destination} (offsets committed)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--idle-exit", type=int, default=0,
                        help="exit after N seconds with no new messages (0 = run forever)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka address, e.g. kafka:9093 when run inside Docker")

    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "parquet-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,     # we commit manually, after writing
    })
    consumer.subscribe([TOPIC])

    rows = []
    batch_started = time.monotonic()
    last_message_at = time.monotonic()

    try:
        while True:
            if not rows:
                batch_started = time.monotonic()

            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print("Kafka error:", msg.error())
                else:
                    last_message_at = time.monotonic()
                    try:
                        rows.append(parse_message(msg))
                    except Exception as e:
                        send_to_dead_letter(msg, e)

            batch_full = len(rows) >= BATCH_SIZE
            batch_old = time.monotonic() - batch_started >= BATCH_SECONDS
            if rows and (batch_full or batch_old):
                flush_batch(consumer, rows)
                rows = []

            if args.idle_exit and time.monotonic() - last_message_at > args.idle_exit:
                break
    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception:
        failed = True
        raise
    finally:
        if rows and not failed:
            flush_batch(consumer, rows)
        consumer.close()


if __name__ == "__main__":
    main()