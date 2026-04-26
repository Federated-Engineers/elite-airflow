import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.liffey_luxury.datasources_to_s3_extract import (
    gsheet_to_s3, postgres_to_s3)
from business_logic.liffey_luxury.transform import transform_and_push_to_s3

logger = logging.getLogger(__name__)


default_args = {
    "owner": "liffey_luxury",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="liffey_luxury_transformation",
    default_args=default_args,
    schedule="0 8 * * *",
    description="Extract data from Postgres and Google Sheets, \
    transform it, and write back to S3 for Liffey Luxury Lounge Client",
    catchup=False,
    tags=["liffey_luxury", "s3", "google_sheets", "postgres"],
) as dag:

    extract_from_gsheet_and_push_to_S3 = PythonOperator(
        task_id="extract_from_gsheet_and_push_to_S3",
        python_callable=gsheet_to_s3,
    )

    extract_from_postgres_and_push_to_S3 = PythonOperator(
        task_id="extract_from_postgres_and_push_to_S3",
        python_callable=postgres_to_s3,
    )

    extract_transform_and_push_to_S3 = PythonOperator(
        task_id="extract_transform_and_push_to_S3",
        python_callable=transform_and_push_to_s3,
    )

[extract_from_gsheet_and_push_to_S3,
 extract_from_postgres_and_push_to_S3,
 ] >> extract_transform_and_push_to_S3
