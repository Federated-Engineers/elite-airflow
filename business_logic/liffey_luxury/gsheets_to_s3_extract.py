import awswrangler as wr
import pandas as pd

#from plugins.google_sheet import get_google_sheets_client
from plugins.date_utils import get_current_datetime
from plugins.google_sheet import get_data_from_gsheet


SSM_PATH = "/production/google-service-account/credentials"
GSHEET_ID = "1OUKw-fYdDN7HQw0hrTnKpP2yY9Gyqk0V4tuPeINWHF4"

BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
FOLDER = "liffey_luxury"

current_time = get_current_datetime()
S3_PATH = f"s3://{BUCKET_NAME}/{FOLDER}/{current_time}_marketing_crm.parquet"

def gsheet_to_s3(gsheet_id: str, s3_path: str):
    """Extract data from a Google Sheet and write to S3 in Parquet format.

    Args:
        gsheet_id: The ID of the Google Sheet to extract data from.
        s3_path: The S3 path (including bucket and prefix) to write the
        Parquet file to.
    """

    data = get_data_from_gsheet(gsheet_id, SSM_PATH)

    df = pd.DataFrame(data)

    wr.s3.to_parquet(df, s3_path)

gsheet_to_s3(GSHEET_ID, S3_PATH)