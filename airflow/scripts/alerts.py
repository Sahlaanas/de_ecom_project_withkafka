"""
on_failure_callback used by every task in the pipeline DAG. Posts a message
to Slack if SLACK_WEBHOOK_URL is set; otherwise just logs.
"""
import os

import requests


def slack_alert_on_failure(context):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context.get("execution_date")
    log_url = context["task_instance"].log_url

    message = (
        f":red_circle: *Airflow task failed*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*When:* {execution_date}\n"
        f"*Logs:* {log_url}"
    )

    if not webhook_url:
        print(f"[alerts] SLACK_WEBHOOK_URL not set, would have sent:\n{message}")
        return

    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except Exception as e:
        print(f"[alerts] failed to post to Slack: {e}")
