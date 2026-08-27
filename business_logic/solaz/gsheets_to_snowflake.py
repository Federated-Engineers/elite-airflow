import logging
import pandas as pd

from airflow.sdk import Variable

from plugins.google_sheet import get_data_from_gsheet
from plugins.snowflake_helper import write_dataframe_to_snowflake

logger = logging.getLogger(__name__)

config = Variable.get("solaz_config", deserialize_json=True)
sensitive_config = Variable.get("solaz_sensitive_config",
                                deserialize_json=True)


def gsheets_to_snowflake():
    """Extract daily orders data from a Google Sheet and write to Snowflake.

    All variables needed for this function are retrieved from Airflow Variables
    The variables include Google Sheet ID, SSM path for Google credentials
    """

    gsheet_id = config["google_sheet"]["sheet_id"]
    sheet_name = config["google_sheet"]["sheet_name"]
    google_ssm_path = sensitive_config["google_ssm_path"]

    logger.info(f"Connecting to Google Sheet with ID: {gsheet_id}")

    data = get_data_from_gsheet(gsheet_id, google_ssm_path, sheet_name)
    logger.info("Data extracted from Google Sheet")


    orders_df = pd.DataFrame(data)
    # Use incremental load


    orders_df.columns = [
        column.upper()
        for column in orders_df.columns
    ]

    logger.info(f"Here are the columns {orders_df.columns}")

    orders_df["TRANSACTION_TIMESTAMP"] = pd.to_datetime(
        orders_df["TRANSACTION_TIMESTAMP"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    orders_df["LOADED_AT"] = pd.Timestamp.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    
    # Connect to Snowflake
    database = config["snowflake"]["database"]
    schema = config["snowflake"]["bronze_schema"]
    table = "ORDERS"

    write_dataframe_to_snowflake(
        df=orders_df,
        table_name=table,
        database=database,
        schema=schema,
    )
   