"""
Runs the Kafka -> S3 Parquet consumer for a bounded window, then exits.
This is what the DAG calls hourly; the long-running `consumer/consumer.py`
(Week 1) is for continuous local dev/demo instead.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "consumer"))

from consumer import build_consumer, build_s3_client, write_batch_to_s3  # noqa: E402
from collections import defaultdict

RUN_DURATION_SECONDS = int(os.getenv("BATCH_RUN_DURATION_SECONDS", "300"))


def run():
    consumer = build_consumer()
    s3_client = build_s3_client()
    buffers = defaultdict(list)
    start = time.time()

    print(f"[consume_batch] running for {RUN_DURATION_SECONDS}s")
    while time.time() - start < RUN_DURATION_SECONDS:
        records = consumer.poll(timeout_ms=1000, max_records=200)
        for _, msgs in records.items():
            for msg in msgs:
                buffers[msg.key].append(msg.value)

    for event_type, records in buffers.items():
        write_batch_to_s3(s3_client, event_type, records)

    consumer.close()
    print("[consume_batch] done")


if __name__ == "__main__":
    run()
