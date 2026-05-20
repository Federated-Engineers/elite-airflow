import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.mare_viva.mare_viva_to_s3_glue import (
    get_config, load_data_to_s3_glue)
from plugins.date_utils import get_current_datetime
from plugins.s3_helper import s3_full_path, write_dataframe_to_s3_glue

default_args = {
    'start_date': datetime.datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
}


dag = DAG(
    dag_id='mare_viva_dag',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to send mare viva data from Supabase to S3',
)


harvest_operator = PythonOperator(
    dag=dag,
    python_callable=write_dataframe_to_s3_glue,
    op_kwargs={
        'df': load_data_to_s3_glue()["harvest_date_partitioned_df"],
        'path': s3_full_path(
            get_config()['bucket_name'], get_config()['folder_name_harvest']
        ),
        'partition_cols': ['year', 'month', 'day'],
        'filename_prefix': get_current_datetime(),
        'database': get_config()['staging_glue_db'],
        'table': 'harvest_lifecycle_record',
        'mode': 'append',
    },
    task_id='harvest_s3_glue_task',
)


lagoon_operator = PythonOperator(
    dag=dag,
    python_callable=write_dataframe_to_s3_glue,
    op_kwargs={
        'df': load_data_to_s3_glue()["lagoon_date_partitioned_df"],
        'path': s3_full_path(
            get_config()['bucket_name'], get_config()['folder_name_lagoon']
        ),
        'partition_cols': ['year', 'month', 'day'],
        'filename_prefix': get_current_datetime(),
        'database': get_config()['staging_glue_db'],
        'table': 'lagoon_environmental_log',
        'mode': 'append',
    },
    task_id='lagoon_s3_glue_task',
)

[harvest_operator, lagoon_operator]
