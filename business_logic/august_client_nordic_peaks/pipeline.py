from config import (
    DATA_SOURCES,
    S3_FOLDER_PATH,
    SERVICE_ACCOUNT_CREDENTIALS_PATH,
)
from module import load_gsheet_to_s3

data_sources_key = ([(source_name, id)
                     for source_name, id in DATA_SOURCES.items()])

load_data = load_gsheet_to_s3(
    googlesheet_id=data_sources_key[0][1],
    ssm_path=SERVICE_ACCOUNT_CREDENTIALS_PATH,
    folder_path=S3_FOLDER_PATH,
    file_name=data_sources_key[0][0]
)

print(load_data)
