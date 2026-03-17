import logging

import awswrangler as wr
import pandas as pd

from plugins.date_utils import get_current_datetime
from plugins.get_gsheet import get_data_from_gsheet

logging.basicConfig(level=logging.INFO)

SHEET_KEY = "1IIr3cYvnT7T7IWMD-naJ-IqghvOgP5aFEybT-7ecO2w"
SSM_PATH = "/production/google-service-account/credentials"

filename = "repairs.csv"
project_dir = "alpen_mechanik"
bucket_name = "federated-engineers-staging-elite-data-lake"
bucket_path = f"{bucket_name}/{project_dir}"

current_time = get_current_datetime()

client_path = f"s3://{bucket_path}/clients/{filename}"
data_team_path = f"s3://{bucket_path}/data_team/{current_time}_{filename}"


paths = [client_path, data_team_path]


def extract_sheet_to_s3_sftp() -> None:
    """
    A function to load data to Amazon S3-SFTP-Server
    Args:
        None
    Return:
        None
    """

    logging.info('Commenced data loading.....')

    logging.info("Storage destinations defined.....")

    data = get_data_from_gsheet(SHEET_KEY, SSM_PATH)

    df = pd.DataFrame(data)

    for path in paths:
        logging.info(f"Writing records to the file path: {path}")
        wr.s3.to_csv(
            df=df,
            path=path,
            index=False,
            dataset=False
        )

    logging.info("Data loaded successfully!!!!!!")
