import logging

import awswrangler as wr
import pandas as pd
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)


config = Variable.get("hg_config", deserialize_json=True)
BUCKET_NAME = config["bucket_name"]
FOLDER_NAME = config["folder_name"]
SSM_PATH = config["ssm_path"]


def _write_sheet_to_s3(sheet_id_variable: str, dataset_name: str):

    sheet_data = get_data_from_gsheet(
        Variable.get(sheet_id_variable), SSM_PATH
    )
    df = pd.DataFrame(sheet_data)

    if df.empty:
        logger.info(f"No data found in {dataset_name}. Skipping S3 write.")
        return

    df = df.drop_duplicates()

    current_date = get_current_datetime()
    s3_path = (
        f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/"
        f"{dataset_name}/{current_date}.parquet"
    )

    wr.s3.to_parquet(df=df, path=s3_path, dataset=False)
    logger.info(f"{dataset_name} data written to S3 at {s3_path}")


def write_lancy_to_s3():
    _write_sheet_to_s3("hg_lancy_sheet_id", "lancy")


def write_rhone_to_s3():
    _write_sheet_to_s3("hg_rhone_sheet_id", "rhone")
