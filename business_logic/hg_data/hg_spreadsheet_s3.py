import awswrangler as wr
import pandas as pd
from airflow.models import Variable

from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet

config = Variable.get("hg_config", deserialize_json=True)
BUCKET_NAME = config["bucket_name"]
FOLDER_NAME = config["folder_name"]
SSM_PATH = config["ssm_path"]


def _write_sheet_to_s3(
    sheet_id_variable: str, dataset_name: str
):
    df = pd.DataFrame(
        get_data_from_gsheet(Variable.get(sheet_id_variable), SSM_PATH)
    )
    if df.empty:
        return print("No data found in the Google Sheet. Skipping S3 write.")

    current_date = get_current_datetime()
    s3_path = (
        f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/"
        f"{dataset_name}/{current_date}.parquet"
    )

    s3_file_path = (
        f"s3://{BUCKET_NAME}/{FOLDER_NAME}/raw/"
        f"{dataset_name}/{current_date}.parquet"
    )
    hg_ingestion = wr.s3.list_objects(s3_file_path)
    if s3_path in hg_ingestion:
        print("File already exists. Skip writing.")
    else:
        wr.s3.to_parquet(df=df, path=s3_path, dataset=False)
        print("File written successfully.")


def write_lancy_to_s3():
    _write_sheet_to_s3("hg_lancy_sheet_id", "lancy")


def write_rhone_to_s3():
    _write_sheet_to_s3("hg_rhone_sheet_id", "rhone")
