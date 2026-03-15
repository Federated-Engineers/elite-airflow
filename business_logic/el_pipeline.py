import gspread
import boto3
import json
import pandas as pd
import datetime
from airflow.sdk import Variable


today = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")


def read_from_gsheet():
    cred_dict = json.loads(Variable.get('credential'))
    gsheet_client = gspread.service_account_from_dict(cred_dict)
    # read a Google Workbook by key
    workbook = gsheet_client.open_by_key(Variable.get("key"))

    worksheet = workbook.sheet1
    data = worksheet.get_all_records()

    df = pd.DataFrame(data)

    return df


def load_to_s3():
    df = read_from_gsheet()

    file_name = 'repairs.csv'
    main_folder = 'alpen_mechanik'
    bucket = Variable.get("bucket")
    csv_data = df.to_csv(index=False)

    s3_client = boto3.client("s3",
                             region_name=("eu-central-1"))

    engr_path = f'{main_folder}/{'alpen_records'}/{today}_{file_name}'
    vendors_path = f'{main_folder}/{'vendor'}/{file_name}'
    paths = [engr_path, vendors_path]

    for path in paths:
        s3_client.put_object(
            Bucket=bucket,
            Key=path,
            Body=csv_data
        )

    print(f'Uploaded No of Records: {df.shape[0]}')


def data_pipeline():

    try:
        print('commenced data ingestion...')
        load_to_s3()
        print("Ingestion complete")
        print('File successfully uploaded')

    except Exception as e:
        print(f'Pipeline failed....{e}')