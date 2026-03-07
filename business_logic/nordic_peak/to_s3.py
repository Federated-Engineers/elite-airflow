from datetime import datetime

import awswrangler as wr
import boto3
from airflow.sdk import Variable

from .google_auth import read_google_sheet


def setup_aws_session() -> None:
    """Setup default AWS session with credentials from Airflow variables."""
    boto3.setup_default_session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name="eu-central-1"
    )


def write_to_s3(sheet_id: str, path_dir: str) -> None:
    """
    Function to read a Google Sheet and write it to S3 as Parquet.
    Args:
        sheet_id: Google Sheet ID to read from
        path_dir: S3 path directory (e.g., 'finance_data',
        'marketing_campaign_data')
    """
    wr.engine.set("python")
    setup_aws_session()
    now = datetime.now().strftime("%Y-%m-%d")
    raw_s3_bucket = "federated-engineers-production-elite-nordic-peak"
    path = f"s3://{raw_s3_bucket}/{path_dir}"
    wr.s3.to_parquet(
        df=read_google_sheet(sheet_id),
        path=path,
        dataset=True,
        mode="append",
        filename_prefix=f"{now}_"
    )
