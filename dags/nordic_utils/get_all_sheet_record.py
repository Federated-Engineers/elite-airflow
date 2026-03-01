import pandas as pd

from .google_auth import get_google_sheets_credentials


def read_google_sheet(spreadsheet_id):
    google_credential = get_google_sheets_credentials()
    read_google_sheet = google_credential.open_by_key(spreadsheet_id)
    df = pd.DataFrame(read_google_sheet.sheet1.get_all_records())
    return df
