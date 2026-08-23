import logging

from datetime import date

import awswrangler as wr
import boto3
import pandas as pd

from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

def load_gsheet_to_s3(
        gsheet_id: str,
        ssm_path: str,
        folder_path: str,
        file_name: str
):
    """
    Function to load googlesheet data to aws bucket

    Args:
        gsheet_id: The Google Sheet ID/key.
        ssm_path: The SSM parameter path where the Google service account
        credentials are stored.
    Returns:
        Path to file in s3
    """

    data = get_data_from_gsheet(
        gsheet_id=gsheet_id,
        ssm_path=ssm_path
    )
    dataframe = pd.DataFrame(data)
    file_path = f"{folder_path}/date={date.today()}/{file_name}"
    wr.s3.to_parquet(
        df=dataframe,
        path=file_path,
        dataset=False
        )

    logging.info(f"{len(dataframe)} records loaded to {folder_path}")
    return file_path