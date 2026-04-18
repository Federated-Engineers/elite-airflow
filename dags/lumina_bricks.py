import datetime
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.lumina import s3_base_path
from plugins.aws import get_ssm_parameter
from plugins.database import db_connection, load_postgres_table_to_s3

DAG_ID = "lumina_brick_properties_pipeline"
db_cred = json.loads(get_ssm_parameter('/supabase/database/credentials'))


default_args = {
    'owner': 'Federated-Engineers',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 3, 5),
    'retries': 3,
    'retry_delay': datetime.timedelta(seconds=6),
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
    python_callable=load_postgres_table_to_s3,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "historical_transactions",
        "base_path": s3_base_path,
        "schema_name": "historical"
    }
)


migrate_property_metadata = PythonOperator(
    dag=dag,
    task_id="property_metadata",
    python_callable=load_postgres_table_to_s3,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "property_metadata",
        "base_path": s3_base_path,
        "schema_name": "historical"
    }
)

migrate_renovation_ledgers = PythonOperator(
    dag=dag,
    task_id="renovation_ledgers",
    python_callable=load_postgres_table_to_s3,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "renovation_ledgers",
        "base_path": s3_base_path,
        "schema_name": "historical"
    }
)

migrate_neighborhood_demographics = PythonOperator(
    dag=dag,
    task_id="neighborhood_demographics",
    python_callable=load_postgres_table_to_s3,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "neighborhood_demographics",
        "base_path": s3_base_path,
        "schema_name": "historical"
    }
)

migrate_zoning_permits = PythonOperator(
    dag=dag,
    task_id="zoning_permits",
    python_callable=load_postgres_table_to_s3,
    op_kwargs={
        "connection": db_connection(db_cred),
        "table_name": "zoning_permits",
        "base_path": s3_base_path,
        "schema_name": "historical"
    }
)

migrate_historical_transactions
migrate_property_metadata
migrate_renovation_ledgers
migrate_neighborhood_demographics
migrate_zoning_permits
