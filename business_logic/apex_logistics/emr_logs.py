import gzip
import logging

from business_logic.apex_logistics import config
from plugins.aws import get_s3_client

logger = logging.getLogger(__name__)


def print_step_logs(cluster_id: str, step_id: str):
    """
    Print the stderr log of an EMR step in the Airflow task log.

    EMR copies step logs from the cluster to the LogUri in S3.

    Args:
        cluster_id: The EMR cluster ID.
        step_id: The EMR step ID.
    """

    key = f"{config.LOG_PREFIX}/{cluster_id}/steps/{step_id}/stderr.gz"

    response = get_s3_client().get_object(Bucket=config.LOG_BUCKET, Key=key)
    step_log = gzip.decompress(response["Body"].read()).decode("utf-8")

    logger.info(f"Step log: s3://{config.LOG_BUCKET}/{key}")
    logger.info(step_log)
