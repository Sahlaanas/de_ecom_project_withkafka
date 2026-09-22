"""
Orchestrates the e-commerce pipeline:
Kafka -> S3 (Parquet) -> Snowflake (COPY INTO) -> dbt build
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT_BIN = "/opt/dbt_venv/bin/dbt"

# Values registered in Admin -> Variables
S3_BUCKET = Variable.get("s3_bucket")
SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": Variable.get("snowflake_account"),
    "SNOWFLAKE_USER": Variable.get("snowflake_user"),
    "SNOWFLAKE_PASSWORD": Variable.get("snowflake_password"),
}

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Kafka -> S3 -> Snowflake -> dbt",
    start_date=datetime(2026, 9, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "production"],
) as dag:

    ingest_to_s3 = BashOperator(
        task_id="ingest_kafka_to_s3",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"python consumer/kafka_to_parquet.py "
            f"--bootstrap-servers kafka:9093 --idle-exit 30"
        ),
        env={"S3_BUCKET": S3_BUCKET, **SNOWFLAKE_ENV},
        append_env=True,
    )

    load_to_snowflake = BashOperator(
        task_id="load_snowflake_raw",
        bash_command=(
            f'{DBT_BIN} run-operation load_new_raw_events '
            f'--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
        ),
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f'{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}'
        ),
        env=SNOWFLAKE_ENV,
        append_env=True,
    )

    ingest_to_s3 >> load_to_snowflake >> dbt_build