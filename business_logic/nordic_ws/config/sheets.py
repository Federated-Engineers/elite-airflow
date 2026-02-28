SHEETS = [
    {"name": "marketing",   "id": "13ULiowSb4JuNIaLldstlNB7sqr1WBe0Yrjm8ALEyLQ0", "s3_folder": "marketing", "unique_keys": ["date", "product_id"]},
    {"name": "supply",     "id": "1MLhga5_FJpgpr8sBLjUlvwQ8sNDPULnv3Rbps3J5l7k", "s3_folder": "supply", "unique_keys": ["date", "product_id"]},
    {"name": "user_growth",  "id": "1xBazE0xUO0sUc7TqsosSCTqe2eQQ1x9-1k5wJemTxc0", "s3_folder": "user_growth", "unique_keys": "user_id"},
    {"name": "finance", "id": "1vQFb4E7QEpVRkpRe34CyGfaH9VJRFVZpFLAc7taYYwg", "s3_folder": "finance", "unique_keys": ["date", "product_id"]}
]

S3_BUCKET   = "federated-engineers-staging-elite-data-lake"
SSM_PARAM   = "/staging/elite/dev/service-account/credentials.json"
AWS_REGION  = "eu-central-1"