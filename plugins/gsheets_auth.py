import json
import logging

import gspread
from google.oauth2.service_account import Credentials

from plugins.aws import get_ssm_parameter

logger = logging.getLogger(__name__)

SSM_PATH = "/production/google-service-account/credentials"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_google_sheets_client():
    """
    Authenticate with Google Sheets using credentials stored in AWS SSM.
    """
    
    credentials_dict = json.loads(get_ssm_parameter(SSM_PATH))

    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=SCOPES,
    )

    logger.info("Google Sheets authentication successful")
    return gspread.authorize(credentials)
