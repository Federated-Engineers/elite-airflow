import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nordic_utils.to_finance_s3 import write_to_finance_s3
from nordic_utils.to_marketing_s3 import write_to_marketing_s3
from nordic_utils.to_supply_chain_s3 import write_to_supply_chain_s3
from nordic_utils.to_user_growth_s3 import write_to_user_growth_s3

default_args = {
    'start_date': datetime.datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'catchup': False
}
dag = DAG(
    dag_id='nordic_firm_dag',
    default_args=default_args,
    schedule='@daily',
    description='A DAG to send google sheets data to S3',
)
write_market_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_marketing_s3,
        task_id='write_marketing_s3'
    )
write_finance_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_finance_s3,
        task_id='write_finance_s3'
    )
write_supply_chain_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_supply_chain_s3,
        task_id='write_supply_chain_s3'
    )
write_growth_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_user_growth_s3,
        task_id='write_user_growth_s3'
    )
write_market_s3 >> write_finance_s3 >> write_supply_chain_s3 >> write_growth_s3
