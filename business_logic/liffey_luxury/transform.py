import awswrangler as wr

from plugins.date_utils import get_current_datetime
from plugins.s3_helper import get_latest_s3_file, write_df_to_s3


BUCKET_NAME = "federated-engineers-staging-elite-data-lake"
FOLDER = "liffey_luxury"


def transform_and_upload():
    df_marketing = wr.s3.read_parquet(path=get_latest_s3_file(BUCKET_NAME, f"{FOLDER}/raw/marketing"))
    df_orders = wr.s3.read_parquet(path=get_latest_s3_file(BUCKET_NAME, f"{FOLDER}/raw/orders"))

    df_transformed = df_orders.merge(df_marketing, on="customer_id", how="left")

    print(write_df_to_s3(df_transformed, BUCKET_NAME, f"{FOLDER}/transformed", f"{get_current_datetime()}_transformed.parquet"))


transform_and_upload()
