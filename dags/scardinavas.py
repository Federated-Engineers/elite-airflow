import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from business_logic.scardinavas.gsheet_to_s3 import gsheet_to_s3_dataset

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
    description='A DAG to send google sheets data to S3',
)

DATASETS = {
    "orders": {
        "path_dir": "orders_data",
        "date_column": "order_date",
    },
    "shipments": {
        "path_dir": "shipments_data",
        "date_column": "shipment_date",
    },
    "payments": {
        "path_dir": "payments_data",
        "date_column": "payment_date",
    },
}

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
