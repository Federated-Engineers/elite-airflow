import json
import logging
import pandas as pd

from airflow.sdk import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import postgres_db_connection, db_query_results_to_df
from plugins.snowflake_helper import establish_snowflake_connection, write_dataframe_to_snowflake


logger = logging.getLogger(__name__)


config = Variable.get("solaz_config", deserialize_json=True)
sensitive_config = Variable.get(
    "solaz_sensitive_config",
    deserialize_json=True,
)


def postgres_to_snowflake():
    """
    Extract tables from PostgreSQL data_factory schema
    and load them into the Snowflake Bronze schema.
    """

    db_ssm_path = sensitive_config["db_ssm_path"]
    db_cred = json.loads(get_ssm_parameter(db_ssm_path))

    postgres_conn = postgres_db_connection(db_cred)
    logger.info("Connected to PostgreSQL database")


    # Load individual tables to Snowflake
    database = config["snowflake"]["database"]
    schema = config["snowflake"]["bronze_schema"]

    snowflake_conn = establish_snowflake_connection()
    tables = [
        "dim_customers",
        "dim_products",
        "app_orders",
    ]

    try:
        for table in tables:

            logger.info(f"Extracting {table} from Postgres")

            query = f"""
                SELECT *
                FROM data_factory.{table};
            """

            df = db_query_results_to_df(connection=postgres_conn, query=query)

            logger.info(f"Extracted {len(df)} rows from data_factory.{table}")

            df["LOADED_AT"] = pd.Timestamp.now().strftime(
                "%Y-%m-%d %H:%M:%S")

            write_dataframe_to_snowflake(
                df=df,
                table_name=table,
                database=database,
                schema=schema,
                connection=snowflake_conn
            )

    finally:
        snowflake_conn.close()
        logger.info("Snowflake connection closed")

        postgres_conn.close()
        logger.info("PostgreSQL connection closed")
