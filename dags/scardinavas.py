import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.scardinavas.gsheet_to_s3 import gsheet_to_s3_dataset
from business_logic.scardinavas.scardinavas_config import DATASETS

CONFIG = Variable.get("scardinavas_config", deserialize_json=True)
SHEETS = Variable.get("scardinavas_sheets", deserialize_json=True)


default_args = {
    'start_date': datetime.datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'catchup': False
}

dag = DAG(
    dag_id='scardinavas_dag',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to send google sheets data to s3',
)

tasks = []
for name, dataset in DATASETS.items():
    task = PythonOperator(
        dag=dag,
        task_id=f"write_{name}_s3",
        python_callable=gsheet_to_s3_dataset,
        op_kwargs={
            "gsheet_id": SHEETS[name],
            "ssm_path": CONFIG["ssm_path"],
            "bucket": CONFIG["bucket"],
            "path_dir": dataset["path_dir"],
            "database": CONFIG["database"],
            "table_name": name,
            "date_column": dataset["date_column"],
        },
    )
    tasks.append(task)

tasks
