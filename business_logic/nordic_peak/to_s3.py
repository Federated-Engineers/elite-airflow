from datetime import datetime

import awswrangler as wr

from .google_auth import read_google_sheet


def write_to_s3(sheet_id: str, path_dir: str) -> None:
    """
    Function to read a Google Sheet and write it to S3 as Parquet.
    Args:
        sheet_id: Google Sheet ID to read from
        path_dir: S3 path directory (e.g., 'finance_data',
        'marketing_campaign_data')
    """
    wr.engine.set("python")
    now = datetime.now().strftime("%Y-%m-%d")
    raw_s3_bucket = "federated-engineers-production-elite-nordic-peak"
    path = f"s3://{raw_s3_bucket}/{path_dir}"
    wr.s3.to_parquet(
        df=read_google_sheet(sheet_id),
        path=path,
        dataset=True,
        mode="append",
        filename_prefix=f"{now}_"
    )
