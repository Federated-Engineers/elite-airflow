from plugins.s3_helper import s3_full_path

DATA_SOURCES = {
    "finance": "1vQFb4E7QEpVRkpRe34CyGfaH9VJRFVZpFLAc7taYYwg",
    "marketing": "13ULiowSb4JuNIaLldstlNB7sqr1WBe0Yrjm8ALEyLQ0",
    "growth": "1xBazE0xUO0sUc7TqsosSCTqe2eQQ1x9-1k5wJemTxc0",
    "supply_chain": "1MLhga5_FJpgpr8sBLjUlvwQ8sNDPULnv3Rbps3J5l7k"
}

SERVICE_ACCOUNT_CREDENTIALS_PATH = (
    "/production/google-service-account/credentials"
    )
BUCKET_NAME = "federated-engineers-production-elite-nordics-peaks-storage"
# BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
S3_FOLDER_PATH = s3_full_path(
    BUCKET_NAME, "raw"
)
