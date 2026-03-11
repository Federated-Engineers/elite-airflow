import awswrangler as wr
import boto3

bucket = "gdm-raw-data"
folder = "daily_extracts"
s3_path = f"s3://{bucket}/{folder}"


def extract_portugal_data():

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=f"{folder}/")

    files = response.get("Contents", [])

    if len(files) == 0:
        raise ValueError(f"No file found in {s3_path}")

    latest_object = max(files, key=lambda x: x["LastModified"])
    latest_file = f"s3://{bucket}/{latest_object['Key']}"
    print(f"Latest file: {latest_file}")

    df = wr.s3.read_parquet(latest_file)
    df_portugal = df[df["plant_country"] == "Portugal"]
    print("Portugal data extracted successfully.")

    return df_portugal