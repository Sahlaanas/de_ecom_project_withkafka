"""
Consumes events from Kafka, buffers them into micro-batches, writes each
batch to Parquet, and uploads it to S3 (or a local MinIO endpoint), partitioned
by event_type / date / hour.

Run:
    python consumer/consumer.py
"""
import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
S3_BUCKET = os.getenv("S3_BUCKET", "your-ecommerce-lake-bucket")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL") or None
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", "60"))


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="ecommerce-consumer-group",
    )


def build_s3_client():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    if S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def write_batch_to_s3(s3_client, event_type: str, records: list[dict]):
    if not records:
        return
    table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    now = datetime.now(timezone.utc)
    key = (
        f"raw/{event_type}/dt={now:%Y-%m-%d}/hr={now:%H}/"
        f"{event_type}_{now:%Y%m%dT%H%M%S}_{len(records)}rows.parquet"
    )
    s3_client.upload_fileobj(buf, S3_BUCKET, key)
    print(f"[consumer] wrote {len(records)} '{event_type}' rows -> s3://{S3_BUCKET}/{key}")


def main():
    consumer = build_consumer()
    s3_client = build_s3_client()
    buffers: dict[str, list[dict]] = defaultdict(list)
    last_flush = time.time()

    print(f"[consumer] listening on '{TOPIC}', flushing every "
          f"{BATCH_INTERVAL_SECONDS}s or {BATCH_SIZE} records/type.")

    def flush_all():
        for event_type, records in list(buffers.items()):
            write_batch_to_s3(s3_client, event_type, records)
            buffers[event_type] = []

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=200)
            for _, msgs in records.items():
                for msg in msgs:
                    buffers[msg.key].append(msg.value)

            for event_type, records in buffers.items():
                if len(records) >= BATCH_SIZE:
                    write_batch_to_s3(s3_client, event_type, records)
                    buffers[event_type] = []

            if time.time() - last_flush >= BATCH_INTERVAL_SECONDS:
                flush_all()
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n[consumer] stopping, flushing remaining buffers...")
        flush_all()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
