import json
import logging

import pandas as pd
from airflow.sdk import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import db_query_results_to_df, postgres_db_connection
from plugins.snowflake_helper import (establish_snowflake_connection,
                                      write_dataframe_to_snowflake)

logger = logging.getLogger(__name__)


config = Variable.get("solaz_config", deserialize_json=True)
sensitive_config = Variable.get(
    "solaz_sensitive_config",
    deserialize_json=True,
)


def get_last_value_in_table(connection, database, schema, table, column):
    """ Get the last/max value in a Snowflake table"""

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT MAX({column})
            FROM {database}.{schema}.{table};
            """
        )

        snowflake_max_value = cursor.fetchone()[0]
        logger.info(
            "The last/max value in %s and %s is %s",
            column,
            table,
            snowflake_max_value,
        )

        return snowflake_max_value

    finally:
        cursor.close()


def get_new_postgres_data(
        connection,
        table,
        column=None,
        data_type=None,
        snowflake_max_value=None,
        ):
    """Check for Postgres data created since the last Snowflake load."""

    # Handles first full load
    query = f"""
        SELECT *
        FROM data_factory.{table}
    """

    # Implements incremental load
    if snowflake_max_value is not None:
        query += f"""
        WHERE {column}::{data_type} > '{snowflake_max_value}'
        """

    query += ";"

    df = db_query_results_to_df(
        connection=connection,
        query=query,
    )

    df.columns = [
        column.upper()
        for column in df.columns
    ]

    return df


def get_existing_product_ids(connection, database, schema):
    """Get Product IDs (SKU IDs) already in Snowflake"""

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT SKU_ID
            FROM {database}.{schema}.DIM_PRODUCTS;"""
        )

        product_ids = {row[0] for row in cursor.fetchall()}
        return product_ids

    finally:
        cursor.close()


def get_new_products(products_df, existing_product_ids):
    """Check for products created since the last Snowflake load."""

    new_products = products_df[
        ~products_df["SKU_ID"].isin(existing_product_ids)
    ].copy()

    logger.info(f"{len(new_products)} new products found")

    return new_products


def postgres_to_snowflake():
    """
    Extract tables from PostgreSQL data_factory schema
    and load them into the Snowflake Bronze schema.
    """

    db_ssm_path = sensitive_config["db_ssm_path"]
    db_cred = json.loads(get_ssm_parameter(db_ssm_path))

    postgres_conn = postgres_db_connection(db_cred)
    logger.info("Connected to PostgreSQL database")

    database = config["snowflake"]["database"]
    schema = config["snowflake"]["bronze_schema"]
    snowflake_conn = establish_snowflake_connection()

    # Load tables with date columns i.e DIM_CUSTOMERS and APP_ORDERS
    tables = {
        "DIM_CUSTOMERS": ("REGISTRATION_DATE", "DATE"),
        "APP_ORDERS": ("TRANSACTION_TIMESTAMP", "TIMESTAMP")
    }

    try:
        for table, (column, data_type) in tables.items():
            snowflake_max_value = get_last_value_in_table(
                snowflake_conn, database, schema, table, column
            )

            df = get_new_postgres_data(
                postgres_conn, table, column, data_type, snowflake_max_value
            )

            if df.empty:
                logger.info(f"No new data to extract from {table}")
                continue

            if snowflake_max_value is None:
                logger.info(
                    "Full load to Snowflake %s rows from %s",
                    len(df),
                    table,
                )
            else:
                logger.info(f"{len(df)} new rows found in {table}")

            df["LOADED_AT"] = pd.Timestamp.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            write_dataframe_to_snowflake(
                df=df,
                table_name=table,
                database=database,
                schema=schema,
            )
            logger.info(f"New data from {table} written to Snowflake")

        # Load tables with no date columns i.e DIM_PRODUCTS
        products_df = get_new_postgres_data(
            postgres_conn, "DIM_PRODUCTS"
        )

        existing_products_ids = get_existing_product_ids(
            snowflake_conn, database, schema
        )

        products_df = get_new_products(products_df, existing_products_ids)

        if products_df.empty:
            logger.info("No new data to extract from DIM_PRODUCTS")
            return

        products_df["LOADED_AT"] = pd.Timestamp.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        write_dataframe_to_snowflake(
            df=products_df,
            table_name="dim_products",
            database=database,
            schema=schema,
        )
        logger.info("New data from dim_products written to Snowflake")

    finally:
        snowflake_conn.close()
        logger.info("Snowflake connection closed")

        postgres_conn.close()
        logger.info("PostgreSQL connection closed")
