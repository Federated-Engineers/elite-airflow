import json

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from plugins.aws import get_ssm_parameter


def get_google_sheets_credentials():
    """Fetches Google Sheets API credentials from AWS Systems
    Manager Parameter Store and returns an authorized gspread client.
    Returns:
        gspread.Client: An authorized gspread client for
        interacting with Google Sheets.
    """
    credentials_dict = get_ssm_parameter(
        "/production/elite/service-account/credentials.json")
    credentials_dict = json.loads(credentials_dict)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(credentials_dict,
                                                        scopes=scopes)
    gc = gspread.authorize(credentials)
    return gc


def read_google_sheet(spreadsheet_id: str):
    """Reads data from a Google Sheet and returns it as a pandas DataFrame.
    Args:
        spreadsheet_id (str): The ID of the Google Sheet to read from.
    Returns:
        pd.DataFrame: A DataFrame containing the data from the Google Sheet.
    """
    google_credential = get_google_sheets_credentials()
    read_google_sheet = google_credential.open_by_key(spreadsheet_id)
    df = pd.DataFrame(read_google_sheet.sheet1.get_all_records())
    return df
