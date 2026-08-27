# import json
import logging

# import awswrangler as wr
import pandas as pd
from airflow.sdk import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# from plugins.aws import get_ssm_parameter
# from plugins.database import postgres_db_connection, db_query_results_to_df
from plugins.google_sheet import get_data_from_gsheet

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


    database = config["snowflake"]["database"]
    schema = config["snowflake"]["bronze_schema"]
    table = config["snowflake"]["orders_table"]

    logger.info("Processing gsheet columns")

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

    logger.info(f"Data types:\n{orders_df.dtypes}")
    
    # Connect to Snowflake
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    conn = snowflake_hook.get_conn()

    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=orders_df,
        table_name=table,
        database=database,
        schema=schema,
        quote_identifiers=False
    )

    if not success:
        raise RuntimeError(
            f"Failed to load orders into {database}.{schema}.{table}"
        )

    logger.info(
        "Successfully loaded %s rows into %s.%s.%s",
        num_rows,
        database,
        schema,
        table
    )
   