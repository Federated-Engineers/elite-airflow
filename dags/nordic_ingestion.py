from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from business_logic.nordic_ws.config.sheets import SHEETS
from business_logic.nordic_ws.src.auth2 import get_gspread_client
from business_logic.nordic_ws.src.ingest import ingest_sheet

default_args = {
    "owner": "elite-data-engineers",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email": ["your-team@email.com"],
}

with DAG(
    dag_id="nordic_peaks_ingestion",
    default_args=default_args,
    description="Daily ingestion from Google Sheets to S3",
    schedule="0 6 * * *",   # runs every day at 6am UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nordic_peaks", "ingestion", "google_sheets"],
) as dag:

    def run_ingest(sheet_config, **kwargs):
        client = get_gspread_client()
        ingest_sheet(client, sheet_config)

    # Dynamically create a task per sheet
    for sheet in SHEETS:
        PythonOperator(
            task_id=f"ingest_{sheet['name']}",
            python_callable=run_ingest,
            op_kwargs={"sheet_config": sheet},
        )
