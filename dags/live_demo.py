import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from business_logic.live_demo import demo

DAG_ID = 'live-demo'


default_args = {
    'owner': 'Federated-Engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
    'execution_timeout': datetime.timedelta(minutes=10)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)


simple_s3_write = PythonOperator(
    dag=dag,
    task_id='simple_s3_write',
    python_callable=demo
)

simple_s3_write
