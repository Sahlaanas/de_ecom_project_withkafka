"""
Hourly pipeline: consume Kafka -> load raw to Snowflake -> dbt run -> dbt test.

Each task retries 3x with exponential backoff and alerts Slack on final failure.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from alerts import slack_alert_on_failure  # noqa: E402
import load_to_snowflake  # noqa: E402

DBT_PROJECT_DIR = "/opt/airflow/dbt_ecommerce"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": slack_alert_on_failure,
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Kafka -> S3 -> Snowflake -> dbt star schema, hourly",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "portfolio"],
) as dag:

    consume_kafka_batch = BashOperator(
        task_id="consume_kafka_batch",
        bash_command="python /opt/airflow/scripts/consume_batch.py",
    )

    load_raw_to_snowflake = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=load_to_snowflake.run,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt deps && "
            "dbt run --profiles-dir /opt/airflow/dbt_ecommerce"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir /opt/airflow/dbt_ecommerce"
        ),
    )

    consume_kafka_batch >> load_raw_to_snowflake >> dbt_run >> dbt_test
