import os
from plugins.aws import get_ssm_parameter

import awswrangler as wr
import json
import boto3
import pandas as pd
import psycopg2

PROJECT_DIR = "LuminaBrick_Properties"
BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
SCHEMA_NAME = "historical"
BUCKET_PATH = f"{BUCKET_NAME}/{PROJECT_DIR}"


database_credentials = json.loads(get_ssm_parameter('/supabase/database/credentials'))

def db_connection():
    return psycopg2.connect(
        host=(database_credentials["host"]),
        dbname=(database_credentials["database_name"]),
        user=(database_credentials["username"]),
        password=(database_credentials["password"]),
    )

s3_base_path = f"s3://{BUCKET_PATH}"


get_tables_to_migrate = [
        "historical_transactions",
        "property_metadata",
        "renovation_ledgers",
        "neighborhood_demographics",
        "zoning_permits",
    ]


def read_from_table(connection, schema_name, table_name):
    """
    Reads data from one table into a Pandas DataFrame.

    """
    query = f'SELECT * FROM "{schema_name}"."{table_name}"'
    return pd.read_sql_query(query, connection)


def load_to_s3(df, base_path, table_name):
    """
    Writes one table as its own Parquet file in S3.

    """
    output_path = f"{base_path}/{table_name}.parquet"

    wr.s3.to_parquet(
        df=df,
        path=output_path,
        dataset=False,
        # boto3_session=boto3.Session(),
    )

    return output_path


def migrate_one_table(schema_name, table_name, base_path):
    print(f"\nMigrating {schema_name}.{table_name}...")

    connection = db_connection()
    try:
        df = read_from_table(connection, schema_name, table_name)
    finally:
        connection.close()

    output_path = load_to_s3(df, base_path, table_name)

    print(f"Done: {output_path}")
    print(f"Rows exported: {len(df)}")


def migrate_all_tables():
    schema_name = "historical"
    base_path = s3_base_path
    tables = get_tables_to_migrate

    for table_name in tables:
        migrate_one_table(schema_name, table_name, base_path)

    print("\nAll tables exported successfully.")


