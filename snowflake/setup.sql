-- Run this once in the Snowflake UI (or via snowsql) as an ACCOUNTADMIN/SYSADMIN.
-- Replace the S3 credentials / bucket path before running.

CREATE WAREHOUSE IF NOT EXISTS ECOMMERCE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS ECOMMERCE;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.STAGING;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE.MARTS;

USE WAREHOUSE ECOMMERCE_WH;
USE DATABASE ECOMMERCE;
USE SCHEMA RAW;

-- 1. File format for the Parquet files the consumer writes
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = PARQUET;

-- 2. External stage pointing at your S3 lake bucket
CREATE OR REPLACE STAGE ecommerce_s3_stage
  URL = 's3://your-ecommerce-lake-bucket/raw/'
  CREDENTIALS = (AWS_KEY_ID = 'your_key' AWS_SECRET_KEY = 'your_secret')
  FILE_FORMAT = parquet_format;
-- Prefer a Storage Integration over inline credentials for anything beyond a demo:
-- https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

-- 3. Raw landing tables — VARIANT column lets Parquet's schema-on-read do the work,
--    so upstream schema drift doesn't break ingestion.
CREATE TABLE IF NOT EXISTS raw.purchase_events (
  raw_data VARIANT,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw.page_view_events (
  raw_data VARIANT,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS raw.inventory_events (
  raw_data VARIANT,
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Load from S3. In production this COPY INTO is what Airflow's
--    `load_raw_to_snowflake` task runs on a schedule (see airflow/scripts/load_to_snowflake.py).
COPY INTO raw.purchase_events (raw_data)
  FROM @ecommerce_s3_stage/purchase/
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'CONTINUE';

COPY INTO raw.page_view_events (raw_data)
  FROM @ecommerce_s3_stage/page_view/
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'CONTINUE';

COPY INTO raw.inventory_events (raw_data)
  FROM @ecommerce_s3_stage/inventory_update/
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'CONTINUE';

-- Sanity check
SELECT COUNT(*) FROM raw.purchase_events;
