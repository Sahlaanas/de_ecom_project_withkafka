import glob
import os

import pyarrow.parquet as pq

files = sorted(glob.glob("data/lake/raw_events/**/*.parquet", recursive=True))
print(f"{len(files)} parquet files")
for f in files[-3:]:
    print(f"  {f}  ({os.path.getsize(f):,} bytes)")

table = pq.read_table("data/lake/raw_events")   # reads the whole folder tree as one table
print("\nSchema:")
print(table.schema)
print(f"\nTotal rows: {table.num_rows}")

event_ids = table["event_id"].to_pylist()
print(f"Distinct event_ids: {len(set(event_ids))}  "
      f"(duplicates: {len(event_ids) - len(set(event_ids))})")

print("\nRows per event type:")
print(table.group_by("event_type").aggregate([("event_id", "count")]).to_pandas()
      if False else table.group_by("event_type").aggregate([("event_id", "count")]).to_pylist())