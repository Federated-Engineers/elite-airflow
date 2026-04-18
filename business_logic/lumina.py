import json

from airflow.models import Variable

from plugins.aws import get_ssm_parameter

config = Variable.get("lumina_bricks_config", deserialize_json=True)
PROJECT_DIR = config["project_dir"]
BUCKET_NAME = config["bucket_name"]
SCHEMA_NAME = config["schema_name"]


BUCKET_PATH = f"{BUCKET_NAME}/{PROJECT_DIR}"

db_cred = json.loads(get_ssm_parameter('/supabase/database/credentials'))

s3_base_path = f"s3://{BUCKET_PATH}"

TABLES_TO_MIGRATE = [
        "historical_transactions",
        "property_metadata",
        "renovation_ledgers",
        "neighborhood_demographics",
        "zoning_permits",
    ]
