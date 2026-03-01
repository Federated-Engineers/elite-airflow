import json

import boto3
from airflow.sdk import Variable


def get_ssm_parameter():
    ssm_parameter = "/staging/elite/dev/service-account/credentials.json"
    client = boto3.client(
                'ssm',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                region_name='eu-central-1',
        )
    response = client.get_parameter(Name=ssm_parameter, WithDecryption=True)
    credentials_json = response['Parameter']['Value']
    return json.loads(credentials_json)
