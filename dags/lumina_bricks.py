import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from business_logic.lumina import migrate_all_tables

DAG_ID = "lumina_brick_properties_pipeline"


default_args = {
    'owner': 'Federated-Engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 3, 5),
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

migrate_all_tables = PythonOperator(
    dag=dag,
    task_id='migrate_all_tables',
    python_callable=migrate_all_tables
)


migrate_all_tables
