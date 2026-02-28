import json
import boto3
import gspread
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from google.oauth2.service_account import Credentials
from business_logic.nordic_ws.config.sheets import SSM_PARAM, AWS_REGION

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]



def get_gspread_client():
    """Fetch service account from SSM and return authenticated gspread client."""
    hook = AwsBaseHook(aws_conn_id="aws_default", client_type="ssm")
    ssm = hook.get_client_type(region_name=AWS_REGION)

    response = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)
    sa_info = json.loads(response["Parameter"]["Value"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)