import json

import gspread
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from business_logic.nordic_ws.config.sheets import AWS_REGION, SSM_PARAM
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]


def get_gspread_client():
    """Fetch service account from SSM and return
    authenticated gspread client."""
    hook = AwsBaseHook(aws_conn_id="aws_default", client_type="ssm")
    ssm = hook.get_client_type(region_name=AWS_REGION)

    response = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)
    sa_info = json.loads(response["Parameter"]["Value"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)
