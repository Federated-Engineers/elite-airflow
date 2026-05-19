import logging

import awswrangler as wr
import pandas as pd
import psycopg2

logging.basicConfig(level=logging.INFO)


def postgres_db_connection(db_cred: dict):
    """
    Create a PostgreSQL database connection.

    Args:
        db_cred (dict): Dictionary containing database credentials:
            {
                "host": str,
                "database_name": str,
                "username": str,
                "password": str,
                "port": int (optional, default=5432)
            }

    Returns:
        connection: psycopg2 connection object
    """
    connection = psycopg2.connect(
        host=db_cred["host"],
        dbname=db_cred["database_name"],
        user=db_cred["username"],
        password=db_cred["password"],
    )
    return connection


def postgres_query_output_to_df(schema: str, table_name: str,   connection):
    """
    A function that extracts data from a specified table
    in the postgres database. It uses the provided database credentials to establish a connection.
    Args:
        schema (str): The schema name.
        table_name (str): The table name.
        connection: For the connection parameter, pass the psycopg2 connection 
        object returned by the postgres_db_connection function.
        That is, you should first call postgres_db_connection with 
        the appropriate database credentials to get the connection object, 
        and then pass that object
        
    Returns:
        pd.DataFrame: The extracted data as a pandas DataFrame.
    """
    query = f"SELECT * FROM {schema}.{table_name};"
    df = pd.read_sql(query, connection)
    return df




def load_postgres_table_to_s3(
        connection: str,
        table_name: str,
        base_path: str,
        schema_name: str,
        dataset=False,
        ):
    """
    This function reads a table from a PostgreSQL database
    and uploads it to S3 in Parquet format.
    Parameters:
    - schema_name: schema where the table exists
    - table_name: table to migrate
    - base_path: S3 destination path
    """
    query = f"SELECT * FROM {schema_name}.{table_name}"
    file_path = f"{base_path}/{table_name}.parquet"

    try:
        logging.info(f"reading table: {table_name}...")

        df = pd.read_sql_query(query, connection)

        logging.info(f"Uploading to S3: {file_path}")
        wr.s3.to_parquet(
            df=df,
            path=file_path,
        )
    except Exception as e:
        logging.error(f"Error migrating {table_name}: {e}")
    finally:
        connection.close()
