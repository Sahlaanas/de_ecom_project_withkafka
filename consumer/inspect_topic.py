"""Quick check: drain the topic and verify key -> partition behaviour."""
from collections import defaultdict

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "inspect-tool-v2",      # fresh group = no saved offsets
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,        # inspection only: never save a bookmark
})
consumer.subscribe(["ecommerce-events"])

partition_counts = defaultdict(int)
key_partitions = defaultdict(set)
no_key = 0
empty_polls = 0

try:
    while empty_polls < 5:              # stop after 5 quiet seconds
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            print("Error:", msg.error())
            continue
        empty_polls = 0
        partition_counts[msg.partition()] += 1
        if msg.key() is None:
            no_key += 1
        else:
            key_partitions[msg.key().decode()].add(msg.partition())
finally:
    consumer.close()

print("Messages per partition:", dict(sorted(partition_counts.items())))
print("Messages without a key:", no_key)
print("Distinct keys seen:", len(key_partitions))
bad = {k: sorted(p) for k, p in key_partitions.items() if len(p) > 1}
print("Keys found on more than one partition:", bad or "none")