import json
import boto3
import gspread
from google.oauth2.service_account import Credentials
from config.sheets import SSM_PARAM, AWS_REGION

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]

def get_gspread_client():
    """Fetch service account from SSM and return authenticated gspread client."""
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    response = ssm.get_parameter(Name=SSM_PARAM, WithDecryption=True)
    sa_info = json.loads(response["Parameter"]["Value"])
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return gspread.authorize(creds)