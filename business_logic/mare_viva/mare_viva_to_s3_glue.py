import json
import logging

from airflow.models import Variable

from plugins.aws import get_ssm_parameter
from plugins.database import (postgres_db_connection,
                              postgres_query_output_to_df)
from plugins.pandas_helper import add_ingestion_timestamp, table_partition_cols

logger = logging.getLogger(__name__)


config = {
    "bucket_name": "federated-engineers-staging-elite-data-lake",
    "folder_name_harvest": "harvest_lifecycle_record",
    "folder_name_lagoon": "lagoon_environmental_log",
    "glue_production_db": "elite-mare-viva",
    "staging_glue_db": "federated-engineers-elite-staging-db",
    "ssm_credentials_link": "/supabase/database/credentials",
}


config = Variable.get("config", deserialize_json=True)
db_cred = json.loads(get_ssm_parameter(config["ssm_credentials_link"]))
connection = postgres_db_connection(db_cred)

harvest_all_data = postgres_query_output_to_df(
    "historical", "harvest_lifecycle_record", connection
    )

lagoon_all_data = postgres_query_output_to_df(
    "historical", "lagoon_environmental_log", connection
    )

harvest_all_data_ingestion_timestamp = add_ingestion_timestamp(
    harvest_all_data
)
lagoon_all_data_ingestion_timestamp = add_ingestion_timestamp(
    lagoon_all_data
)

harvest_date_partitioned = table_partition_cols(
    harvest_all_data_ingestion_timestamp
)
lagoon_date_partitioned = table_partition_cols(
    lagoon_all_data_ingestion_timestamp
)
