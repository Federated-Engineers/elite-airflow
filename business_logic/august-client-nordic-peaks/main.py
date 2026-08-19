import logging

import awswrangler as wr
import pandas as pd
from plugins.google_sheet import get_data_from_gsheet

from config import DATE_OF_INGESTION

logger = logging.getLogger(__name__)


def load_gsheet_to_s3(
        googlesheet_id: str,
        ssm_path: str,
        s3_folder_path: str,
        file_name: str
        ) -> str:

    """
    Function to write googlesheet data to s3 bucket

    Args:
        googlesheet_id (str): spreadsheet id
        ssm_path (str): path to credential in AWS Parameter store
        s3_folder_path (str): full path to folder in s3
        file_name (str): name of file in bucket

    Returns:
        str: object's full path
    """

    data = get_data_from_gsheet(googlesheet_id, ssm_path)

    file_path = (
        f"{s3_folder_path}/date={DATE_OF_INGESTION}/{file_name}.parquet"
        )

    dataframe = pd.DataFrame(data)

    wr.s3.to_parquet(
        df=dataframe,
        index=False,
        path=file_path,
        dataset=False,
        )

    logging.info(
        f"{len(dataframe)} records from {file_name} "
        "loaded to {s3_folder_path}/{DATE_OF_INGESTION}"
        )
    return file_path
