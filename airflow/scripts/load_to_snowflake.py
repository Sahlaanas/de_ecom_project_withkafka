"""
Runs the S3 -> Snowflake COPY INTO statements for each raw event table.
Called by the Airflow DAG's `load_raw_to_snowflake` task.
"""
import os

import snowflake.connector

COPY_STATEMENTS = {
    "purchase_events": "purchase",
    "page_view_events": "page_view",
    "inventory_events": "inventory_update",
}


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMMERCE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE"),
        schema="RAW",
    )


def run():
    conn = get_connection()
    try:
        cur = conn.cursor()
        for table, s3_prefix in COPY_STATEMENTS.items():
            sql = f"""
                COPY INTO raw.{table} (raw_data)
                FROM @ecommerce_s3_stage/{s3_prefix}/
                FILE_FORMAT = (FORMAT_NAME = parquet_format)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE';
            """
            cur.execute(sql)
            result = cur.fetchall()
            print(f"[load_to_snowflake] {table}: {result}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
