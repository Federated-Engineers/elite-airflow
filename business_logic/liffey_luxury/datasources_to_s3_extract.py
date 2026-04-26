import logging

import awswrangler as wr
import pandas as pd
from airflow.models import Variable
from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_variable():
    """Get all necessary variables from Airflow Variables"""

    config = Variable.get("liffey_luxury_config", deserialize_json=True)
    if not config:
        logger.error("Liffey_luxury_config is missing.")
        raise ValueError("Configuration variables are missing")

    secrets = Variable.get("liffey_luxury_secrets", deserialize_json=True)
    if not secrets:
        logger.error("Liffey_luxury_secrets is missing.")
        raise ValueError("Secrets variables are missing")

    return config, secrets


current_time = get_current_datetime()

config, secrets = get_variable()
bucket_name = config["s3"]["bucket_name"]
folder_name = config["s3"]["base_folder"]


def gsheet_to_s3():
    """Extract data from a Google Sheet and write to S3 in Parquet format.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials,
    S3 bucket and folder information.
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    ssm_path = secrets["google_ssm_path"]

    data = get_data_from_gsheet(gsheet_id, ssm_path)
    logger.info(f"Data extracted from Google Sheet with ID: {gsheet_id}")

    df_marketing = pd.DataFrame(data)

    if df_marketing.empty:
        logger.warning("No data found in the Google Sheet.")
        raise ValueError("No data to write to S3.")

    file_name = f"{current_time}_marketing_crm.parquet"
    marketing_folder = config["s3"]["marketing_folder"]
    marketing_s3_path = (f"s3://{bucket_name}/{folder_name}/"
                         f"{marketing_folder}/{file_name}")

    wr.s3.to_parquet(df_marketing, marketing_s3_path)

    logger.info(f"Data written to S3: {marketing_s3_path}")


def postgres_to_s3():
    """Extract data from a PostgreSQL database and write to S3 in
    Parquet format.

    Args:
        url: The database connection URL.
        query: The SQL query to execute against the PostgreSQL
        database. Defaults to a query that selects all data from
        the orders table.
    """

    logger.info("Starting PostgreSQL to S3 extraction.")
    url = secrets["database_url"]
    query = config["postgres"]["orders_query"]

    if not url:
        logger.error("Database connection URL is not set.")
        raise ValueError("Database connection URL is missing")

    logger.info("Connecting to PostgreSQL database.")
    engine = create_engine(url)

    df_orders = pd.read_sql(query, engine)
    logger.info(f"Data extracted from PostgreSQL: {df_orders.shape[0]} rows")

    if df_orders.empty:
        logger.warning("No data found in the PostgreSQL query.")
        raise ValueError("No data to write to S3.")

    file_name = f"{current_time}_orders.parquet"
    orders_folder = config["s3"]["orders_folder"]
    orders_s3_path = (f"s3://{bucket_name}/{folder_name}/"
                      f"{orders_folder}/{file_name}")

    wr.s3.to_parquet(df_orders, orders_s3_path)
    logger.info(f"Data written to S3: {orders_s3_path}")
