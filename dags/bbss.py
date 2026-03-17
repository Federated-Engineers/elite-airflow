from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.bbss_project.bbss_weatherapi import fetch_weatherapi_data

default_args = {
    'owner': 'federatedengineers',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bbss_weather_data_api_data_pipeline',
    default_args=default_args,
    description='fetch weather_api data daily and save to S3',
    schedule='0 10,16 * * *',
    start_date=datetime(2026, 3, 6),
    catchup=False,
    tags=['weatherapi', 'bbss', 'data_pipeline']
)

task_weatherapi = PythonOperator(
    task_id='fetch_weatherapi_data',
    python_callable=fetch_weatherapi_data,
    dag=dag
)

task_weatherapi
