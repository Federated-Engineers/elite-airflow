import json
import logging

import gspread

from aws import get_ssm_parameter


def get_data_from_gsheet(gsheet_id: str, ssm_paramter_name: str) -> list:
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

    gsheet_credential = json.loads(get_ssm_parameter(ssm_paramter_name))

    gsheet_client = gspread.service_account_from_dict(gsheet_credential)

    workbook = gsheet_client.open_by_key(gsheet_id)
    worksheet = workbook.sheet1

    logging.info('Data Ingestion Completed')
    data = worksheet.get_all_records()

    return data
