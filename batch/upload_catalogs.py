"""Uploads the batch catalogs (products, customers) to S3 as dated snapshots."""
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ["S3_BUCKET"]
CATALOGS = {
    "products": "data/products.json",
    "customers": "data/customers.json",
}


def main():
    s3 = boto3.client("s3")
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for name, path in CATALOGS.items():
        key = f"raw_catalogs/{name}/snapshot_date={snapshot_date}/{name}.json"
        s3.upload_file(path, BUCKET, key)
        print(f"Uploaded {path} -> s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()