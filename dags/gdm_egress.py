from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.gdm.gdm_script import extract_portugal_data

# spreadsheet_id = Variable.get("portugal_spreadsheet_id")
# worksheet_name = Variable.get("portugal_worksheet_name")

spreadsheet_id = "1EJZr940oGXcAVr0KS5nMhmh7VzdYixojkrwquYpZyXw"
worksheet_name = "Sheet1"

default_args = {
    "owner": "gdm",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gdm_egress",
    default_args=default_args,
    schedule="0 8 * * *",
    description="Extract Portugal data from S3 and push to Google Sheets",
    catchup=False,
    tags=["gdm", "egress"],
) as dag:

    extract_and_push_to_sheet = PythonOperator(
        task_id="extract_and_push_to_sheet",
        python_callable=extract_portugal_data,
        op_kwargs={
            "spreadsheet_id": spreadsheet_id,
            "worksheet_name": worksheet_name,
        },
    )
