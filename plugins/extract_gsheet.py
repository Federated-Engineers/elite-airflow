import json
import logging

import boto3
import gspread


def get_ssm_parameter(ssm_name: str):
    """Fetch the value of a parameter from AWS Systems Manager Parameter Store.
    Args:
        ssm_parameter_name (str): The name of the parameter to fetch.
    Returns:
        str: The value of the specified parameter.
    """
    client = boto3.client(
                'ssm',
                region_name='eu-central-1',
        )
    response = client.get_parameter(Name=ssm_name, WithDecryption=True)
    ssm_params_value = response['Parameter']['Value']
    return ssm_params_value


def get_data_from_gsheet(gsheet_id: str, ssm_name: str) -> list:
    """
    A function that ingests data from Googlesheet given the gsheet_id
    Args:
        String
            - gsheet_id: The Googlesheet id/key
            - ssm_name: The SSM Paramter Name
    Returns:
        List of records
            - A List of lists
    """

    logging.info('Starting data ingestion from Googlesheet')

    gsheet_credential = json.loads(get_ssm_parameter(ssm_name))

    gsheet_client = gspread.service_account_from_dict(gsheet_credential)

    workbook = gsheet_client.open_by_key(gsheet_id)
    worksheet = workbook.sheet1

    logging.info('Data Ingestion Completed')
    data = worksheet.get_all_records()

    return data
