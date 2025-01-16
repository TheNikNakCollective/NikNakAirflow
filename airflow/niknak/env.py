import os
from dotenv import load_dotenv

load_dotenv()

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
LOCALSTACK_URL = os.environ.get("LOCALSTACK_URL")
AWS_PROFILE_NAME = os.environ.get("AWS_PROFILE_NAME", None)
S3_ENDPOINT_URL = LOCALSTACK_URL if LOCALSTACK_URL != None else None
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
NIKNAK_RAW_DATA_FOLDER: str = os.environ.get("NIKNAK_RAW_DATA_FOLDER")
NIKNAK_CDN_DATA_FOLDER: str = os.environ.get("NIKNAK_CDN_DATA_FOLDER")