import boto3
from airflow.sdk import Variable


def get_ssm_parameter(ssm_parameter_name: str):
    """Fetch the value of a parameter from AWS Systems Manager Parameter Store.
    Args:
        ssm_parameter_name (str): The name of the parameter to fetch.
    Returns:
        str: The value of the specified parameter.
    """
    ssm_parameter = ssm_parameter_name
    client = boto3.client(
                'ssm',
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
                region_name='eu-central-1',
        )
    response = client.get_parameter(Name=ssm_parameter, WithDecryption=True)
    ssm_params_value = response['Parameter']['Value']
    return ssm_params_value
