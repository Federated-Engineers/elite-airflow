import os
from dotenv import load_dotenv                                                                           
load_dotenv()

import awswrangler as wr
import pandas as pd

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
    df_marketing = pd.DataFrame(data)

    wr.s3.to_parquet(df_marketing, s3_path)

gsheet_to_s3(GSHEET_ID, MARKETING_S3_PATH)


def postgres_to_s3():
    """Extract data from a PostgreSQL database and write to S3 in Parquet format.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL is missing")
    
    engine = create_engine(db_url)

    query = """
        SELECT *
        FROM historical.liffey_luxury_order_transactions
        LIMIT 100;
    """

    df_orders = pd.read_sql(query, engine)
    wr.s3.to_parquet(df_orders, ORDERS_S3_PATH)

postgres_to_s3()


MARKETING_PREFIX = f"s3://{BUCKET_NAME}/{FOLDER}/raw/marketing/"
ORDERS_PREFIX = f"s3://{BUCKET_NAME}/{FOLDER}/raw/orders/"
TRANSFORMED_S3_PATH = f"s3://{BUCKET_NAME}/{FOLDER}/transformed/{current_time}_transformed.parquet"


def get_last_modified_path(prefix: str) -> str:
    objects = wr.s3.describe_objects(prefix)
    last_modified_key = max(objects, key=lambda k: objects[k]["LastModified"])
    return last_modified_key


def join_and_upload():
    marketing_path = get_last_modified_path(MARKETING_PREFIX)
    orders_path = get_last_modified_path(ORDERS_PREFIX)

    df_marketing = wr.s3.read_parquet(path=marketing_path)
    df_orders = wr.s3.read_parquet(path=orders_path)

    df_transformed = df_orders.merge(df_marketing, on="customer_id", how="left")

    wr.s3.to_parquet(df_transformed, TRANSFORMED_S3_PATH)
    print(f"Written to {TRANSFORMED_S3_PATH}")
