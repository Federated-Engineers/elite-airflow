from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from business_logic.client_alpenmechanik.module import (
    email_failure_alert, email_success_alert, load_gsheet_to_s3,
    slack_hook_failure_callback, slack_hook_success_callback)

default_args = {
    "owner": "client_alpenmechanik",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email": [Variable.get("alert_email")],
}


with DAG(
    dag_id="client_alpenmechanik",
    description=(
        "extract repair data from googlesheet "
        "to s3 storage as backend for SFTP server"
    ),
    start_date=datetime(2026, 9, 7),
    schedule="30 7 * * *",
    catchup=False,
    on_success_callback=[slack_hook_success_callback, email_success_alert],
    on_failure_callback=[slack_hook_failure_callback, email_failure_alert],
    default_args=default_args
):

    extract_to_s3 = PythonOperator(
            task_id="extract_sheet_to_s3",
            email=[Variable.get("alert_email")],
            email_on_failure=True,
            python_callable=load_gsheet_to_s3,
            op_kwargs={
                "googlesheet_id": "{{ var.value.data_source}}",
                "ssm_path": "{{ var.value.service_account_details }}",
                "folder_path": "{{ var.value.folder_path }}",
                "file_name": "repairdetails",
                "partition_date": "{{ logical_date | ds }}",
                "run_id": "{{ run_id }}"
            },
        )

extract_to_s3
