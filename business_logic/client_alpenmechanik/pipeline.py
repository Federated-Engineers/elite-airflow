from config import (DATA_SOURCE, S3_FOLDER_PATH,
                    SERVICE_ACCOUNT_CREDENTIALS_PATH)
from module import load_gsheet_to_s3

load_data = load_gsheet_to_s3(
    googlesheet_id=DATA_SOURCE,
    ssm_path=SERVICE_ACCOUNT_CREDENTIALS_PATH,
    folder_path=S3_FOLDER_PATH,
    file_name="repairdetails"
)

print(load_data)
