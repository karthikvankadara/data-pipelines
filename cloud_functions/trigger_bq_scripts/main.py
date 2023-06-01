from google.cloud import bigquery, secretmanager
from google.oauth2.service_account import Credentials
import os
import json

PROJECT_ID = "gcp-mcu-prod"
CREDENTIALS_SECRET = "sa-thanos-secret"
# Use this scoping if you are executing scripts involving external tables using Google Drive sheets
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery"
]

# Construct a BigQuery client object.
def get_credentials_via_api():

    secret_manager_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{PROJECT_ID}/secrets/{CREDENTIALS_SECRET}/versions/latest"
    secret_response = secret_manager_client.access_secret_version(name=secret_name)
    secret_payload = secret_response.payload.data.decode("UTF-8")
    credentials = Credentials.from_service_account_info(json.loads(secret_payload),scopes=SCOPES)
    return credentials

def get_credentials_via_service_account_env():

    secret = os.getenv(f'{CREDENTIALS_SECRET}')  
    credentials = Credentials.from_service_account_info(json.loads(secret), scopes=SCOPES)
    return credentials

def get_credentials_via_service_account_mnt():
    secret_location = f"/secrets/{CREDENTIALS_SECRET}"
    with open(secret_location, "r") as file:
        secret = file.read()
    credentials = Credentials.from_service_account_info(json.loads(secret), scopes=SCOPES)
    return credentials


def main(event, context):    
    #chose one of the three methods to get credentials
    credentials = get_credentials_via_api()
    client = bigquery.Client(project=credentials.project_id,credentials=credentials)
    query_job = client.query("select 'success' as tested")
    query_job.result()