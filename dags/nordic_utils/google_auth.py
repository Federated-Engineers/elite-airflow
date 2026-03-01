import gspread
from google.oauth2.service_account import Credentials

from .get_ssm_parameter import get_ssm_parameter


def get_google_sheets_credentials():
    credentials_dict = get_ssm_parameter()
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(credentials_dict,
                                                        scopes=scopes)
    gc = gspread.authorize(credentials)
    return gc
