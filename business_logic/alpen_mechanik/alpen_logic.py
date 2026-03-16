import json
import logging
from datetime import datetime

import awswrangler as wr
import boto3
import gspread
import pandas as pd

logging.basicConfig(level=logging.INFO)


def get_current_datetime():
    """
    A function that returns the current date and time of ingestion
    Args:
        None
    Returns
        string_datetime
    """
    logging.info("Getting the current date and time of ingestion")
    current_datetime = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    return current_datetime


def read_data_from_ssm(ssm_name: str) -> str:
    logging.info(f'Reading credential from the ssm filename: {ssm_name}')
    ssm_client = boto3.client('ssm')

    ssm_value = ssm_client.get_parameter(Name=ssm_name, WithDecryption=True)
    logging.info(f'credentials retrieved successfully from {ssm_name}')

    return ssm_value


def get_googlesheet_crendentials(secret_name: str) -> dict:
    """
    A function that gets the googlesheet credentials given the name
    Args:
        String (Name of the SSM Parameter)
    Returns:
        Dict (Value of the SSM Parameter)
    """

    cred_dict = read_data_from_ssm(secret_name)
    gsheet_credentials = cred_dict['Parameter']['Value']

    logging.info('Saved and returned the Googlesheet credentials')

    return gsheet_credentials


def get_data_from_gsheet(gsheet_id: str) -> list:
    """
    A function that ingests data from Googlesheet given the gsheet_id
    Args:
        String (The Googlesheet id)
    Returns:
        List of records
    """

    logging.info('Starting data ingestion from Googlesheet')

    ssm_path = "/production/google-service-account/credentials"
    gsheet_credential_json = get_googlesheet_crendentials(ssm_path)
    gsheet_key = json.loads(gsheet_credential_json)
    gsheet_client = gspread.service_account_from_dict(gsheet_key)

    workbook = gsheet_client.open_by_key(gsheet_id)
    worksheet = workbook.sheet1

    logging.info('Data Ingestion Completed')
    data = worksheet.get_all_records()

    return data


def load_to_s3(data) -> None:
    """
    A function to load data to Amazon S3
    Args:
        List
    Return:
        None
    """
    current_time = get_current_datetime()

    logging.info('Commenced data loading.....')

    df = pd.DataFrame(data)

    filename = "repairs.csv"
    project_dir = "alpen_mechanik"
    bucket_name = "federated-engineers-staging-elite-data-lake"
    bucket_path = f"{bucket_name}/{project_dir}"

    client_path = f"s3://{bucket_path}/clients/{filename}"
    data_team_path = f"s3://{bucket_path}/data_team/{current_time}_{filename}"

    logging.info("Storage destinations defined.....")
    paths = [client_path, data_team_path]

    for path in paths:
        wr.s3.to_csv(
            df=df,
            path=path,
            index=False,
            dataset=False
        )

    logging.info("Data loaded successfully!!!!!!")


def alpen_elt_pipeline():
    data = get_data_from_gsheet("1IIr3cYvnT7T7IWMD-naJ-IqghvOgP5aFEybT-7ecO2w")
    load_to_s3(data)
