from plugins.s3_helper import s3_full_path

DATA_SOURCES={
    "finance": "",
    "marketing": "",
    "growth": "",
    "supply_chain": ""
}

SERVICE_ACCOUNT_CREDENTIALS_PATH = "/production/google-service-account/credentials"
BUCKET_NAME = "federated-engineers-production-elite-nordics-peaks-storage"
S3_FOLDER_PATH = s3_full_path(
    BUCKET_NAME, "raw"
)
