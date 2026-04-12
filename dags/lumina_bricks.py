import datetime
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.lumina import migrate_table, s3_base_path
from plugins.aws import get_ssm_parameter
from plugins.psycopg2_connection import db_connection

DAG_ID = "lumina_brick_properties_pipeline"
db_cred = json.loads(get_ssm_parameter('/supabase/database/credentials'))


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
    max_active_runs=1,
    catchup=False,
    tags=[DAG_ID]
)

migrate_historical_transactions = PythonOperator(
    dag=dag,
    task_id="historical_transactions",
    python_callable=migrate_table,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "historical_transactions",
        "base_path": s3_base_path,
    }
)


migrate_property_metadata = PythonOperator(
    dag=dag,
    task_id="property_metadata",
    python_callable=migrate_table,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "property_metadata",
        "base_path": s3_base_path,
    }
)

migrate_renovation_ledgers = PythonOperator(
    dag=dag,
    task_id="renovation_ledgers",
    python_callable=migrate_table,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "renovation_ledgers",
        "base_path": s3_base_path,
    }
)

migrate_neighborhood_demographics = PythonOperator(
    dag=dag,
    task_id="neighborhood_demographics",
    python_callable=migrate_table,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "neighborhood_demographics",
        "base_path": s3_base_path,
    }
)

migrate_zoning_permits = PythonOperator(
    dag=dag,
    task_id="zoning_permits",
    python_callable=migrate_table,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "zoning_permits",
        "base_path": s3_base_path,
    }
)

migrate_historical_transactions
migrate_property_metadata
migrate_renovation_ledgers
migrate_neighborhood_demographics
migrate_zoning_permits
