import sys
sys.path.append("/home/taofeecoh/federated/nordic/elite-airflow")

from module import load_gsheet_to_s3
from config import DATA_SOURCES, SERVICE_ACCOUNT_CREDENTIALS_PATH, S3_FOLDER_PATH

data_sources_key = [(source_name, id) for source_name, id in DATA_SOURCES.items()]

load = load_gsheet_to_s3(
    googlesheet_id=data_sources_key[0][1],
    ssm_path=SERVICE_ACCOUNT_CREDENTIALS_PATH,
    s3_folder_path=S3_FOLDER_PATH,
    file_name=data_sources_key[0][0]
)
