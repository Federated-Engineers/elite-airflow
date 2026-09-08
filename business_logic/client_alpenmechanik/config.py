from plugins.s3_helper import s3_full_path

DATA_SOURCE = "1IIr3cYvnT7T7IWMD-naJ-IqghvOgP5aFEybT-7ecO2w"

SERVICE_ACCOUNT_CREDENTIALS_PATH = (
    "/production/google-service-account/credentials"
    )
BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
# "federated-engineers-production-elite-client-alpenmechanik"

S3_FOLDER_PATH = s3_full_path(
    BUCKET_NAME, "repairpartner"
)
