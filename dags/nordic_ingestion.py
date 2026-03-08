import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.nordic_peak.to_s3 import write_to_s3

default_args = {
    'start_date': datetime.datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
    'catchup': False
}

dag = DAG(
    dag_id='nordic_firm_dag',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to  send google sheets data to S3',
)

write_market_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_s3,
        task_id='write_marketing_s3',
        op_kwargs={
            "sheet_id": "13ULiowSb4JuNIaLldstlNB7sqr1WBe0Yrjm8ALEyLQ0",
            "path_dir": "marketing_campaign_data"
        }
    )

write_finance_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_s3,
        task_id='write_finance_s3',
        op_kwargs={
            "sheet_id": "1vQFb4E7QEpVRkpRe34CyGfaH9VJRFVZpFLAc7taYYwg",
            "path_dir": "finance_data"
        }
    )

write_supply_chain_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_s3,
        task_id='write_supply_chain_s3',
        op_kwargs={
            "sheet_id": "1MLhga5_FJpgpr8sBLjUlvwQ8sNDPULnv3Rbps3J5l7k",
            "path_dir": "supply_chain_data"
        }
    )

write_growth_s3 = PythonOperator(
        dag=dag,
        python_callable=write_to_s3,
        task_id='write_user_growth_s3',
        op_kwargs={
            "sheet_id": "1xBazE0xUO0sUc7TqsosSCTqe2eQQ1x9-1k5wJemTxc0",
            "path_dir": "user_growth_data"
        }
    )

[write_market_s3, write_finance_s3, write_supply_chain_s3, write_growth_s3]
