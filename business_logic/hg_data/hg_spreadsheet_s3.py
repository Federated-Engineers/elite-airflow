import logging

import awswrangler as wr
import pandas as pd
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

logger = logging.getLogger(__name__)


config = Variable.get("hg_config", deserialize_json=True)
BUCKET_NAME = config["bucket_name"]
FOLDER_NAME = config["folder_name"]
SSM_PATH = config["ssm_path"]


def write_sheet_to_s3(sheet_id_variable: str, dataset_name: str):

    sheet_data = get_data_from_gsheet(
        Variable.get(sheet_id_variable), SSM_PATH
    )
    df = pd.DataFrame(sheet_data)

    if df.empty:

        raise AirflowSkipException(f"No data found for {dataset_name}.")

    hg_path = f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/{dataset_name}/"

    try:
        existing_df = wr.s3.read_parquet(hg_path)
    except Exception:
        existing_df = pd.DataFrame()

    if not existing_df.empty:
        duplicates = df.merge(existing_df, how="inner")

        if not duplicates.empty:
            logger.warning(
                f"Duplicate data found for {dataset_name}. Skipping S3 write."
            )
            return

    current_date = get_current_datetime()
    s3_path = f"{hg_path}{current_date}.parquet"

    wr.s3.to_parquet(df=df, path=s3_path, dataset=False)

    logger.info(f"{dataset_name} data successfully written to S3 at {s3_path}")
