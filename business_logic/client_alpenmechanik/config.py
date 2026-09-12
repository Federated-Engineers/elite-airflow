from airflow.models import Variable

DATA_SOURCE = Variable.get("data_source")
SERVICE_ACCOUNT_CREDENTIALS_PATH = (
    Variable.get("service_account_credentials")
    )
FOLDER_PATH = Variable.get("folder_path")
ALERT_EMAIL = Variable.get("alert_email")
