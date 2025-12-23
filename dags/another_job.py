import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_ID = 'Federated-Engineers-Demo-2'


def demo():
    return "@" * 100


default_args = {
    'owner': 'Federated-Engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 15),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=5),
    'execution_timeout': datetime.timedelta(minutes=10)
}


dag = DAG(
    DAG_ID,
    default_args=default_args,
    # schedule_interval='13 8,18 * * 1-6',
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)


get_latest_report_file_task = PythonOperator(
    dag=dag,
    task_id='get_latest_report_file',
    python_callable=demo
)
