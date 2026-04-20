import json

from plugins.aws import get_ssm_parameter


def get_supabase_credentials():
    credentials_dict = get_ssm_parameter("/supabase/database/credentials")
    return json.loads(credentials_dict)
