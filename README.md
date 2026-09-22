# Real-Time E-Commerce & Inventory Analytics Platform

An end-to-end data platform that simulates a mid-size e-commerce company's data stack:
streaming clickstream/order/inventory events land in a data lake, get modeled into a
star schema in a cloud warehouse, and the whole thing runs on an automated, tested,
containerized orchestration layer.

Every piece in this repo runs. Clone it, add your own AWS/Snowflake credentials, and
the pipeline goes from a simulated event to a query-ready fact table on its own.

```
Python event generator
        │
        ▼
    Apache Kafka  (topic: ecommerce-events, 3 partitions, KRaft mode)
        │
        ▼
Kafka → Parquet consumer  (micro-batches, at-least-once, dead-letter handling)
        │
        ▼
    AWS S3  (raw_events/ partitioned by ingest_date/ingest_hour, Parquet)
        │
        ▼
    Snowflake  (storage integration, external stage, COPY INTO — idempotent)
        │
        ▼
    dbt  (staging → star schema, tests, docs)
        │
        ▼
fact_orders · dim_customers · dim_products · dim_date

Orchestrated end-to-end by Apache Airflow (Dockerized, hourly schedule)
```

---

## Why it's built this way

A few deliberate design decisions, since these are usually what come up in interviews:

- **Streaming *and* batch, on purpose.** Clickstream/order/inventory events are
  continuous, so they go through Kafka. Product and customer catalogs change slowly
  and arrive as dated snapshots, the way a batch export from a CRM or PIM system
  would. The platform mirrors how these two patterns coexist in a real company.
- **ELT, not ETL.** Raw Parquet lands in S3 and Snowflake untouched (`payload` stays
  as a JSON string), and all transformation happens in the warehouse via dbt. If a
  transformation has a bug, the raw data is still intact and nothing needs to be
  re-ingested.
- **At-least-once delivery, handled deliberately.** Kafka offsets are only committed
  *after* a Parquet file is written and uploaded, so a crash mid-batch causes
  re-reads, not data loss. The generator even injects duplicate `event_id`s on
  purpose so the dbt layer has something real to deduplicate (`QUALIFY
  ROW_NUMBER() ... = 1`) and test (`unique` on `event_id`).
- **`fact_orders` grain: one row per order.** Every column is chosen to make sense
  at that grain. Surrogate keys (hashed via `dbt_utils.generate_surrogate_key`)
  decouple the warehouse from source-system IDs.
- **Type 1 dimensions.** `dim_customers` and `dim_products` overwrite on change
  rather than tracking history (Type 2). That's a real trade-off, and dbt snapshots
  are the natural next step if history mattered here.
- **No long-lived cloud credentials in the warehouse.** Snowflake reads from S3
  through a storage integration: an IAM role it's allowed to assume, gated by an
  external ID. No AWS keys are ever pasted into Snowflake.
- **Least-privilege IAM throughout.** The app's IAM user can only read/write the
  `raw_events/` and `raw_catalogs/` prefixes of one bucket. Snowflake's role can
  only *read* those same prefixes. Neither can touch anything else in the account.
- **Ingestion-time partitioning (`ingest_date`/`ingest_hour`), not event-time.**
  Late-arriving events never require rewriting old partitions; the pipeline only
  ever appends. Analytics queries use the actual `event_timestamp` column instead.
- **Idempotent everywhere.** Re-running the S3 upload overwrites the same key.
  `COPY INTO` remembers which files it already loaded. `dbt build` rebuilds tables
  deterministically. Airflow retries are safe because every step tolerates re-runs.

---

## Tech stack

| Layer | Tool |
|---|---|
| Event simulation | Python, Faker |
| Streaming | Apache Kafka (KRaft mode, Dockerized) |
| Stream processing | `confluent-kafka` consumer, micro-batched |
| Data lake | AWS S3, Apache Parquet (via PyArrow) |
| Warehouse | Snowflake |
| Transformation & modeling | dbt (dbt-snowflake), star schema |
| Orchestration | Apache Airflow (CeleryExecutor, Dockerized, custom image) |
| Infra | Docker / Docker Compose |

---

## Repository layout

```
.
├── producer/
│   ├── generate_events.py      # simulates clicks, orders, inventory updates
│   └── kafka_producer.py       # publishes simulated events to Kafka
├── consumer/
│   ├── kafka_to_parquet.py     # Kafka -> partitioned Parquet -> S3, at-least-once
│   ├── inspect_topic.py        # diagnostic: key/partition/offset checks
│   └── inspect_lake.py         # diagnostic: schema, row counts, duplicates
├── batch/
│   └── upload_catalogs.py      # uploads product/customer catalogs as dated snapshots
├── docker/
│   └── docker-compose.yml      # local Kafka (KRaft mode)
├── dbt/
│   └── ecom_analytics/
│       ├── models/
│       │   ├── staging/        # sources, cleaning, deduplication
│       │   └── marts/          # dim_customers, dim_products, dim_date, fact_orders
│       ├── macros/             # load_new_raw_events (wraps the raw COPY INTO)
│       ├── tests/              # custom singular tests
│       └── dbt_project.yml
├── airflow/
│   ├── Dockerfile              # extends apache/airflow with pipeline dependencies
│   ├── docker-compose.yaml     # Airflow (Celery executor), remapped to avoid port clashes
│   └── dags/
│       └── ecommerce_pipeline.py   # ingest -> load -> dbt build, hourly
├── requirements.txt
├── .env.example
└── README.md
```

---

## Running it locally

### Prerequisites

- Docker + Docker Compose
- Python 3.10+
- An AWS account with an S3 bucket (Free Tier is enough)
- A Snowflake account (the free trial works)

### 1. Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # fill in S3_BUCKET, SNOWFLAKE_* credentials
aws configure                # your IAM user's access key, secret, region
```

### 2. Generate and stream events

```bash
cd docker && docker compose up -d && cd ..
# create the topic once:
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic ecommerce-events --partitions 3 --replication-factor 1

python producer/kafka_producer.py --rate 20 --max-events 500
python consumer/kafka_to_parquet.py --idle-exit 30
python batch/upload_catalogs.py
```

### 3. Load and model in Snowflake

Run the setup SQL in a Snowflake worksheet (warehouse, database, schemas, storage
integration, stage — see `dbt/ecom_analytics/macros/load_new_raw_events.sql` for the
load logic), then:

```bash
cd dbt/ecom_analytics
dbt deps
dbt build
dbt docs generate && dbt docs serve --port 8081
```

### 4. Orchestrate with Airflow

```bash
cd airflow
docker compose build
docker compose up -d
```

Open the UI, register your S3/Snowflake credentials as Airflow Variables
(`s3_bucket`, `snowflake_account`, `snowflake_user`, `snowflake_password`), unpause
`ecommerce_pipeline`, and trigger a run.

---

## Data quality

Every model in the staging and marts layers is tested with dbt:

- `unique` / `not_null` on every primary and surrogate key
- `relationships` tests proving every fact row points at a real dimension row
  (Snowflake doesn't enforce foreign keys, so this is the substitute)
- `accepted_values` on categorical fields (`order_status`, `device`, `reason`, …)
- `dbt_utils.accepted_range` on quantities and prices
- A custom test asserting `total_amount == quantity * unit_price` on every order

`dbt build` runs models and their tests together, in dependency order, and fails
loudly rather than silently loading bad data.

---

## What I'd change at production scale

- Multi-broker Kafka with a real replication factor, instead of one local broker
- Type 2 dimensions (via dbt snapshots) to preserve customer/product history
- Managed Kafka (MSK/Confluent Cloud) and managed Airflow (MWAA/Composer) instead
  of self-hosted Docker containers
- Incremental dbt models for `fact_orders` instead of a full rebuild each run, once
  data volume made full refreshes too slow
- An "Unknown" member row in each dimension instead of nullable foreign keys, so
  orphaned facts are still queryable without null-handling everywhere

---

## Screenshots


<img width="1777" height="888" alt="image" src="https://github.com/user-attachments/assets/2e7b2d8c-3f21-44cf-89ef-ad405dc16330" />

