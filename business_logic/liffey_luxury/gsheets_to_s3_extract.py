import os
from dotenv import load_dotenv                                                                           
load_dotenv()

import logging

import awswrangler as wr
import pandas as pd

logger = logging.getLogger(__name__)

from sqlalchemy import create_engine
from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet


SSM_PATH = "/production/google-service-account/credentials"
GSHEET_ID = "1OUKw-fYdDN7HQw0hrTnKpP2yY9Gyqk0V4tuPeINWHF4"

BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
FOLDER = "liffey_luxury"

current_time = get_current_datetime()
MARKETING_S3_PATH = f"s3://{BUCKET_NAME}/{FOLDER}/raw/marketing/{current_time}_marketing_crm.parquet"
ORDERS_S3_PATH = f"s3://{BUCKET_NAME}/{FOLDER}/raw/orders/{current_time}_orders.parquet"


def gsheet_to_s3(gsheet_id: str, s3_path: str):
    """Extract data from a Google Sheet and write to S3 in Parquet format.

    Args:
        gsheet_id: The ID of the Google Sheet to extract data from.
        s3_path: The S3 path (including bucket and prefix) to write the
        Parquet file to.
    """

    data = get_data_from_gsheet(gsheet_id, SSM_PATH)
    logger.info(f"Data extracted from Google Sheet with ID: {gsheet_id}")

    df_marketing = pd.DataFrame(data)

    if df_marketing.empty:
        logger.warning("No data found in the Google Sheet.")
        raise ValueError("No data to write to S3.")

    wr.s3.to_parquet(df_marketing, s3_path)
    logger.info(f"Data written to S3: {s3_path}")

gsheet_to_s3(GSHEET_ID, MARKETING_S3_PATH)


# PostgreSQL extraction
query = """
    SELECT *
    FROM historical.liffey_luxury_order_transactions
    LIMIT 100;
"""

def postgres_to_s3(url: str, query: str, s3_path: str):
    """Extract data from a PostgreSQL database and write to S3 in Parquet format.
    """

    logger.info("Starting PostgreSQL to S3 extraction.")

    db_url = os.getenv(url)
    if not db_url:
        logger.error("Database environment variable is not set.")
        raise ValueError("Database environment variable is missing")
    
    logger.info("Connecting to PostgreSQL database.")
    engine = create_engine(db_url)

    df_orders = pd.read_sql(query, engine)
    logger.info(f"Data extracted from PostgreSQL: {df_orders.shape[0]} rows")

    if df_orders.empty:
        logger.warning("No data found in the PostgreSQL query.")
        raise ValueError("No data to write to S3.")

    wr.s3.to_parquet(df_orders, s3_path)
    logger.info(f"Data written to S3: {s3_path}")

postgres_to_s3("DATABASE_URL", query, ORDERS_S3_PATH)
