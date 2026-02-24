from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.auth import get_gspread_client
from src.ingest import ingest_sheet
from config.sheets import SHEETS

default_args = {
    "owner": "elite-data-engineers",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["your-team@email.com"],
}

with DAG(
    dag_id="nordic_peaks_ingestion",
    default_args=default_args,
    description="Daily ingestion from Google Sheets to S3",
    schedule_interval="0 6 * * *",   # runs every day at 6am UTC
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

ingest_revenue
ingest_supply
ingest_finance
ingest_user_growth