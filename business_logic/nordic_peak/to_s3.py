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
        path_dir: S3 path directory (e.g., 'finance_data', 'marketing_campaign_data')
    """
    wr.engine.set("python")
    setup_aws_session()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    raw_s3_bucket = "federated-engineers-staging-elite-data-lake"
    path = f"s3://{raw_s3_bucket}/{path_dir}/{now}"
    
    wr.s3.to_parquet(
        df=read_google_sheet(sheet_id),
        path=path,
        dataset=True,
        mode="append"
    )


def write_to_finance_s3() -> None:
    write_to_s3("1vQFb4E7QEpVRkpRe34CyGfaH9VJRFVZpFLAc7taYYwg", "finance_data")


def write_to_marketing_s3() -> None:
    write_to_s3("13ULiowSb4JuNIaLldstlNB7sqr1WBe0Yrjm8ALEyLQ0", "marketing_campaign_data")


def write_to_supply_chain_s3() -> None:
    write_to_s3("1MLhga5_FJpgpr8sBLjUlvwQ8sNDPULnv3Rbps3J5l7k", "supply_chain_data")


def write_to_user_growth_s3() -> None:
    write_to_s3("1xBazE0xUO0sUc7TqsosSCTqe2eQQ1x9-1k5wJemTxc0", "user_growth_data")
