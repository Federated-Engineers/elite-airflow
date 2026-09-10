from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.client_alpenmechanik.config import (
    DATA_SOURCE, S3_FOLDER_PATH, SERVICE_ACCOUNT_CREDENTIALS_PATH)
from business_logic.client_alpenmechanik.module import load_gsheet_to_s3

default_args = {
    "owner": "client_alpenmechanik",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "email": ["adesanutaofeecoh@gmail.com"],
    "email_on_failure": True,
    "catchup": False,
}


with DAG(
    dag_id="client_alpenmechanik",
    start_date=datetime(2026, 9, 7),
    schedule="0 12 * * *",
    default_args=default_args
):

    extract_sheet = PythonOperator(
            task_id="extract_sheet",
            python_callable=load_gsheet_to_s3,
            op_kwargs={
                "googlesheet_id": DATA_SOURCE,
                "ssm_path": SERVICE_ACCOUNT_CREDENTIALS_PATH,
                "folder_path": S3_FOLDER_PATH,
                "file_name": "repairdetails",
            },
        )

extract_sheet
