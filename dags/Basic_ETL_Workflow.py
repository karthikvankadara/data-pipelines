from airflow import DAG
from airflow.models import Variable
import pendulum
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage

config = Variable.get(
    "etl_config", deserialize_json=True
)

query = "{% include '$sql_script.sql' %}"

SOURCE_BUCKET = config["Source_Bucket"]
PROJECT_ID = config["Project_ID"]
TEMPLATE_SEARCHPATH = config["Template_Searchpath"]
STAGING_DATASET = config["Staging_Dataset"]
TARGET_DATASET = config["Target_Dataset"]
STAGING_FOLDER = config["Staging_Folder"]
ARCHIVE_FOLDER = config["Archive_Folder"]

default_args = {
    'owner': 'karthikv',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 22, tzinfo=pendulum.timezone("Australia/Sydney")),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

# params dictionary would be used in the BigQueryInsertJobOperator to pass parameters
# in the sql file for transforming the staging data
params = {
    "Project_ID":PROJECT_ID,
    "Staging_Dataset":STAGING_DATASET,
    "Target_Dataset":TARGET_DATASET
}

dag = DAG(
    dag_id="Basic_ETL_Workflow",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath=TEMPLATE_SEARCHPATH
)


def _branch(file_pattern):
    prefix = '{}/{}'.format(STAGING_FOLDER,file_pattern)
    print(prefix)
    client = storage.Client(PROJECT_ID)
    blobs = client.list_blobs(SOURCE_BUCKET, prefix = prefix)
    success_file = [blob.name for blob in blobs if '.csv' in blob.name]
    if not success_file:
        return 'no_file_exists'
    else:
        return 'load_csv_to_staging'

with dag:
    start = DummyOperator(task_id="start")
    
    end = DummyOperator(task_id="end", trigger_rule="one_success")
    
    no_file_exists = DummyOperator(task_id = "no_file_exists")

    check_for_data_file = BranchPythonOperator(
        task_id = "check_for_data_file",
        python_callable= _branch,
        op_kwargs={"file_pattern":"netflix_movies"}
    )

    load_csv_to_staging = GCSToBigQueryOperator(
        task_id='load_csv_to_staging',
        bucket=SOURCE_BUCKET,
        source_objects=["{}/netflix_movies_*.csv".format(STAGING_FOLDER)],
        destination_project_dataset_table='{}.{}.netflix_movies_stg'.format(PROJECT_ID,STAGING_DATASET),
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        schema_object="schema_stg/netflix_movies_stg.json",        
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False
    )

    load_target_table = BigQueryInsertJobOperator(
        task_id="load_target_table",
        configuration={
            "query":{
                "query":query.replace("$sql_script","netflix_movies"),
                "useLegacySql":False
            }
        },
        params=params
    )    

    move_files_to_archive =  GCSToGCSOperator(
        task_id="move_files_to_archive",
        move_object=True, 
        source_bucket=SOURCE_BUCKET,
        destination_bucket=SOURCE_BUCKET,
        source_object="{}/netflix_movies".format(STAGING_FOLDER),
        destination_object="{}/".format(ARCHIVE_FOLDER)
    )    

    start >> check_for_data_file >> [load_csv_to_staging, no_file_exists]
    load_csv_to_staging >> load_target_table >> move_files_to_archive >> end
    no_file_exists >> end