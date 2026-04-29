import logging

import awswrangler as wr
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)


current_time = get_current_datetime()

config = Variable.get("liffey_luxury_config", deserialize_json=True)
sensitive_config = Variable.get("liffey_luxury_sensitive_config",
                                deserialize_json=True)

bucket_name = config["s3"]["bucket_name"]
folder_name = config["s3"]["base_folder"]


def gsheet_to_s3():
    """Extract data from a Google Sheet and write to S3 in Parquet format.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials,
    S3 bucket and folder information.
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    ssm_path = sensitive_config["google_ssm_path"]

    logger.info(f"Connecting to Google Sheet with ID: {gsheet_id}")
    
    data = get_data_from_gsheet(gsheet_id, ssm_path)
    logger.info(f"Data extracted from Google Sheet")

    df_marketing = pd.DataFrame(data)

    if df_marketing.empty:
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
    url = sensitive_config["database_url"]
    query = config["postgres"]["orders_query"]

    logger.info("Connecting to PostgreSQL database.")
    engine = create_engine(url)

    df_orders = pd.read_sql(query, engine)
    logger.info(f"Data extracted from PostgreSQL: {df_orders.shape[0]} rows")

    if df_orders.empty:
        raise ValueError("No data to write to S3.")

    file_name = f"{current_time}_orders.parquet"
    orders_folder = config["s3"]["orders_folder"]
    orders_s3_path = (f"s3://{bucket_name}/{folder_name}/"
                      f"{orders_folder}/{file_name}")

    wr.s3.to_parquet(df_orders, orders_s3_path)
    logger.info(f"Data written to S3: {orders_s3_path}")
