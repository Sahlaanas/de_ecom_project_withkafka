# Real-Time E-Commerce & Inventory Analytics Platform

A portfolio-grade data engineering project: streaming ingestion (Kafka) → data lake
(S3, Parquet) → warehouse modeling (Snowflake + dbt, star schema) → orchestration
(Airflow, Dockerized) with tests, retries, and alerting.

This repo is a **working scaffold**, not a toy. Every piece runs. Fill in your own
AWS/Snowflake credentials and it goes end-to-end.

---

## 0. Prerequisites

- Docker + Docker Compose
- Python 3.10+
- A free AWS account (S3 bucket) — Free Tier covers this easily
- A free Snowflake trial account (30 days, $400 credit) — https://signup.snowflake.com
- (Optional) A Slack webhook URL, for failure alerts

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your real credentials
```

---

## Week 1 — Ingestion & Storage

**Goal:** generate realistic events, stream them through Kafka, land them in S3 as Parquet.

```bash
docker compose up -d zookeeper kafka        # brings up local Kafka
python producer/producer.py                 # Terminal 1: generates events continuously
python consumer/consumer.py                 # Terminal 2: consumes, batches, writes Parquet to S3
```

- `producer/producer.py` emits three event types: `page_view`, `purchase`, `inventory_update`,
  as JSON, to Kafka topic `ecommerce_events`.
- `consumer/consumer.py` reads in micro-batches (configurable size/interval), converts to
  Parquet with PyArrow, and uploads to `s3://<bucket>/raw/<event_type>/dt=YYYY-MM-DD/hr=HH/`.
- No AWS account yet? Set `S3_ENDPOINT_URL` in `.env` to use local MinIO instead
  (`docker compose up -d minio`) — same code path, zero cloud cost while you build.

**Checkpoint:** you should see partitioned `.parquet` files landing in your bucket every
`BATCH_INTERVAL_SECONDS`.

---

## Week 2 — Warehousing & Modeling (Snowflake + dbt)

**Goal:** turn raw Parquet into a clean star schema with tests.

1. In the Snowflake UI, run `snowflake/setup.sql` — creates the warehouse, database,
   schemas, an S3 external stage, and raw tables (`raw.orders`, `raw.customers`, `raw.products`,
   `raw.inventory`) loaded via `COPY INTO` from your S3 bucket.
2. Configure dbt:
   ```bash
   cd dbt_ecommerce
   cp profiles.yml.example ~/.dbt/profiles.yml   # fill in your Snowflake creds
   dbt debug
   dbt run
   dbt test
   ```
3. Models built:
   - `staging/stg_orders.sql`, `stg_customers.sql`, `stg_products.sql` — cleaned, typed, deduped
   - `marts/fact_orders.sql` — grain: one row per order line item
   - `marts/dim_customers.sql`, `marts/dim_products.sql` — SCD-1 dimensions
   - `schema.yml` — `unique` + `not_null` tests on every primary key, `relationships`
     tests on foreign keys

**Checkpoint:** `dbt test` passes; you can query `fact_orders` joined to both dimensions
in the Snowflake UI.

---

## Week 3 — Orchestration (Airflow, Dockerized)

**Goal:** one DAG that runs the whole pipeline on a schedule, with retries and alerts.

```bash
docker compose up -d   # brings up Kafka + Airflow (webserver, scheduler, postgres)
# Airflow UI: http://localhost:8080  (user: admin / pass: admin)
```

DAG: `airflow/dags/ecommerce_pipeline_dag.py`, scheduled hourly:

```
consume_kafka_batch  ->  load_raw_to_snowflake  ->  dbt_run  ->  dbt_test
```

- Each task has `retries=3`, exponential backoff, and a `on_failure_callback` that
  posts to Slack (set `SLACK_WEBHOOK_URL` in `.env` / Airflow Variables).
- Task logs are visible per-run in the Airflow UI — use this to practice debugging
  DAG failures, which is a very common interview question.

**Checkpoint:** trigger the DAG manually in the UI, watch all 4 tasks go green, then
kill Kafka mid-run and confirm the retry + Slack alert fires.

---

## How this gets you hired faster

Recruiters/interviewers for data engineering roles are pattern-matching for a small set
of signals. This project hits all of them:

| Signal they look for | Where it shows up here |
|---|---|
| Can you handle streaming data? | Kafka producer/consumer, micro-batching |
| Do you understand the medallion/lake pattern? | Raw Parquet in S3, partitioned by date/hour |
| Can you model data properly? | Star schema (fact + dims), not just raw dumps |
| Do you write tests? | dbt `unique`/`not_null`/`relationships` tests |
| Can you productionize, not just script? | Airflow DAG, retries, alerting, Docker |
| Do you know a real cloud warehouse? | Snowflake (very in-demand vs. toy SQLite demos) |

**Resume bullets you can use once this runs:**
- "Built an end-to-end streaming data pipeline (Kafka → S3 → Snowflake) processing
  simulated e-commerce events, modeled as a star schema in dbt with automated data
  quality tests."
- "Orchestrated a 4-stage ETL/ELT pipeline in Airflow with retry logic and Slack alerting,
  containerized with Docker for reproducible local development."

**GitHub polish that matters:**
- Put this README (trimmed) at the repo root with an architecture diagram (draw one in
  ~15 min with excalidraw.com — screenshot it in).
- Record a 60–90s Loom walking through the Airflow UI with the DAG succeeding — link it
  at the top of the README. This is what actually gets watched.
- Pin the repo on your GitHub profile.

**Interview talking points to rehearse:**
- Why Parquet over JSON/CSV in the lake (columnar, compression, schema).
- Why partition by date/hour (partition pruning, cost/performance).
- Why a star schema over raw normalized tables for analytics (join simplicity, query perf).
- What happens on a late-arriving event / out-of-order data (mention idempotent upserts,
  `MERGE` in dbt incremental models — see `fact_orders.sql` comments for a starter).
- What you'd change for real production scale (managed Kafka/MSK or Kinesis, Airflow on
  MWAA/Composer, incremental dbt models instead of full refresh).

---

## Suggested pace (fits your 4-week plan)

- **Days 1–5:** Week 1 tasks above, get comfortable with Kafka locally.
- **Days 6–12:** Week 2, Snowflake + dbt, don't skip the tests.
- **Days 13–19:** Week 3, Airflow DAG + failure handling.
- **Days 20–28:** Polish — README, diagram, Loom video, resume bullets, post on
  LinkedIn/GitHub, start applying while still iterating.

Don't wait for "done" to start applying — start once Week 2's checkpoint passes.
A working pipeline with a couple of rough edges beats a perfect one you never ship.
