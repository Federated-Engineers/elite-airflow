import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.mare_viva.mare_viva_to_s3_glue import (
    extract_harvest_to_s3_to_glue, extract_lagoon_to_s3_to_glue)

default_args = {
    'start_date': datetime.datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
}


dag = DAG(
    dag_id='mare_viva_dag',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to  send mare viva data from Supabase to S3',
)

write_harvest_s3_glue = PythonOperator(
        dag=dag,
        python_callable=extract_harvest_to_s3_to_glue,
        task_id='write_harvest_s3_glue',
    )

write_lagoon_s3_glue = PythonOperator(
        dag=dag,
        python_callable=extract_lagoon_to_s3_to_glue,
        task_id='write_lagoon_s3_glue',
    )

[write_harvest_s3_glue, write_lagoon_s3_glue]
