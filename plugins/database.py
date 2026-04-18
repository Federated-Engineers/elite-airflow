import logging

import awswrangler as wr
import pandas as pd
import psycopg2

logging.basicConfig(level=logging.INFO)


def db_connection(db_cred: dict):
    """
    Creates a PostgreSQL database connection.

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

    return psycopg2.connect(
        host=db_cred["host"],
        dbname=db_cred["database_name"],
        user=db_cred["username"],
        password=db_cred["password"],
    )


def load_postgres_table_to_s3(
        connection: str,
        table_name: str,
        base_path: str,
        schema_name: str,
        ):
    """
This function reads a table from PostgreSQL and
uploads it to a S3 destination in Parquet format.
Parameters:
- schema_name: schema where the table exists
- table_name: table to migrate
- base_path: S3 destination path
- connection: psycopg2 connection object
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
            dataset=False,
        )
    except Exception as e:
        logging.error(f"Error migrating {table_name}: {e}")
    finally:
        connection.close()
