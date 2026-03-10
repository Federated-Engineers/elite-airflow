import awswrangler as wr
import pandas as pd

bucket = "gdm-raw-data"
folder = "daily_extracts"

s3_path = f"s3://{bucket}/{folder}"


def extract_portugal_data(s3_path):
    files = wr.s3.list_objects(s3_path, include_details=True)
    if len(files) == 0:
        raise ValueError(f"No file found in {s3_path}")

    sorted_files = sorted(files, key=lambda x: x['LastModified'], reverse=True)
    latest_file = sorted_files[0]['Key']
    #print(latest_file)

    df = wr.s3.read_parquet(f"s3://{latest_file}")
    df_portugal = df[df["plant_country"] == "Portugal"]

    print(df_portugal)


extract_portugal_data(s3_path)
# print(df.head())
