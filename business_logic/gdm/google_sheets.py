import json

import gspread
from google.oauth2.service_account import Credentials

from plugins.aws import get_ssm_parameter


def get_google_sheets_client():
    """
    Authenticate with Google Sheets using credentials stored in AWS SSM.
    """

    credentials_dict = get_ssm_parameter(
        "/production/google-service-account/credentials")
    credentials_dict = json.loads(credentials_dict)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"]

    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=scopes)

    return gspread.authorize(credentials)


def append_dataframe_to_sheet(df, spreadsheet_id, worksheet_name):
    """
    Append dataframe rows to Google Sheets.
    """

    gc = get_google_sheets_client()
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)

    rows = df.values.tolist()
    worksheet.append_rows(rows)
