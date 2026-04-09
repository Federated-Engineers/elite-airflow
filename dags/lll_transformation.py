import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.liffey_luxury.datasources_to_s3_extract import gsheet_to_s3, postgres_to_s3
from business_logic.liffey_luxury.transform import transform_and_upload

logger = logging.getLogger(__name__)

GSHEET_ID = Variable.get("gsheet_id")
DATABASE_URL = Variable.get("database_url")


default_args = {
    "owner": "lll",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lll_transformation",
    default_args=default_args,
    schedule="0 8 * * *",
    description="Extract data from Postgres and Google Sheets, transform it, and write back to S3 for Liffey Luxury Lounge Client",
    catchup=False,
    tags=["lll", "s3", "google_sheets", "postgres"],
) as dag:

    extract_from_gsheet_and_push_to_S3 = PythonOperator(
        task_id="extract_from_gsheet_and_push_to_S3",
        python_callable=gsheet_to_s3,
        op_kwargs={
            "gsheet_id": GSHEET_ID,
        },
    )

    extract_from_postgres_and_push_to_S3 = PythonOperator(
        task_id="extract_from_postgres_and_push_to_S3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "url": DATABASE_URL,
        },
    )

    extract_transform_and_push_to_S3 = PythonOperator(
        task_id="extract_transform_and_push_to_S3",
        python_callable=transform_and_upload,
    )

[extract_from_gsheet_and_push_to_S3, extract_from_postgres_and_push_to_S3] >> extract_transform_and_push_to_S3
