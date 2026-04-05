import logging

import awswrangler as wr
import pandas as pd
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)

# Load configuration
config = Variable.get("hg_config", deserialize_json=True)
BUCKET_NAME = config["bucket_name"]
FOLDER_NAME = config["folder_name"]
SSM_PATH = config["ssm_path"]


def _write_sheet_to_s3(sheet_id_variable: str, dataset_name: str):
    # Read data from Google Sheet
    df = pd.DataFrame(get_data_from_gsheet(Variable.get(sheet_id_variable), SSM_PATH))

    if df.empty:
        logger.info(f"No data found in {dataset_name}. Skipping S3 write.")
        return

    # Remove duplicates just in case
    df = df.drop_duplicates()

    # Generate timestamped file path
    current_date = get_current_datetime()
    s3_path = f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/{dataset_name}/{current_date}.parquet"

    # Write to S3
    wr.s3.to_parquet(df=df, path=s3_path, dataset=False)
    logger.info(f"{dataset_name} data written to S3 at {s3_path}")


def write_lancy_to_s3():
    _write_sheet_to_s3("hg_lancy_sheet_id", "lancy")


def write_rhone_to_s3():
    _write_sheet_to_s3("hg_rhone_sheet_id", "rhone")
