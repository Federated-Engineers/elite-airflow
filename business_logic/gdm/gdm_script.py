import awswrangler as wr

bucket = "gdm-raw-data"
folder = "daily_extracts"

s3_path = f"s3://{bucket}/{folder}"


def extract_portugal_data(s3_path):
    files = wr.s3.list_objects(s3_path)
    if len(files) == 0:
        raise ValueError(f"No file found in {s3_path}")
    
    metadata = wr.s3.describe_objects(files)
    latest_file = max(metadata, key=lambda x: metadata[x]['LastModified'])

    df = wr.s3.read_parquet(f"s3://{latest_file}")
    df_portugal = df[df["plant_country"] == "Portugal"]

    # df_portugal.to_csv("portugal_data.csv", index=False)

    return(df_portugal)

extract_portugal_data(s3_path)