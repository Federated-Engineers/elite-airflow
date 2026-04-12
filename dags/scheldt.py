import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from business_logic.scheldt_river.google_sheet_to_s3 import extract_sheet,transform_dim_product,transform_fact_orders

default_args = {
    'start_date': datetime.datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
    'catchup': False
}

dag = DAG(
    dag_id='scheldt_rivers_logistics',
    default_args=default_args,
    schedule='0 8 * * *',
    description='A DAG to  extract data from google sheets to S3 and Glue Database',
)

extract_products = PythonOperator(
    dag=dag,
    task_id="extract_products",
    python_callable=extract_sheet,
    op_kwargs={
        "sheet_name": "Products_Data",
        "table_name": "raw_products",
        "s3_prefix": "raw/products",
    },
)

extract_orders = PythonOperator(
    dag=dag,
    task_id="extract_orders",
    python_callable=extract_sheet,
    op_kwargs={
        "sheet_name": "Orders_Data",
        "table_name": "raw_orders",
        "s3_prefix": "raw/orders",
    },
)

extract_payments = PythonOperator(
    dag=dag,
    task_id="extract_payments",
    python_callable=extract_sheet,
    op_kwargs={
        "sheet_name": "Payments_Received",
        "table_name": "raw_payments",
        "s3_prefix": "raw/payments",
    },
)

transform_dim_product = PythonOperator(
    dag=dag,
    task_id="transform_dim_product",
    python_callable=transform_dim_product,
)

transform_fact_orders = PythonOperator(
    dag=dag,
    task_id="transform_fact_orders",
    python_callable=transform_fact_orders,
)

extract_products >> transform_dim_product
[extract_orders,extract_payments] >> transform_fact_orders
