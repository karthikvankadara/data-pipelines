from airflow import DAG
from airflow.models import Variable
import pendulum
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.weight_rule import WeightRule
from google.cloud import storage

config = Variable.get(
    "etl_config", deserialize_json=True
)

query = "{% include '$sql_script.sql' %}"

DATA_IMPORT_TABLES_LIST = Variable.get("data_import_tables_list")
SOURCE_BUCKET = config["Source_Bucket"]
PROJECT_ID = config["Project_ID"]
TEMPLATE_SEARCHPATH = config["Template_Searchpath"]
STAGING_DATASET = config["Staging_Dataset"]
TARGET_DATASET = config["Target_Dataset"]
STAGING_FOLDER = config["Staging_Folder"]
ARCHIVE_FOLDER = config["Archive_Folder"]
MAIN_DAG_ID = "Trigger_Daily_Batch_Loads"

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

# Helper methods for subdag and subtask creation
def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval, template_searchpath):
  ''' Returns a DAG which has the dag_id formatted as parent.child '''
  return DAG(
    dag_id='{}.{}'.format(parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    template_searchpath=template_searchpath,
    start_date=start_date,
    default_args=default_args,
    max_active_runs=15
  )

def _branch(file_pattern):
    ''' Checks to see if given table's load files are present in the landing folder '''
    # look for files with table_name pattern
    prefix = '{}/{}'.format(STAGING_FOLDER,file_pattern)
    print(prefix)
    client = storage.Client(PROJECT_ID)
    blobs = client.list_blobs(SOURCE_BUCKET, prefix = prefix)
    success_file = [blob.name for blob in blobs if '.csv' in blob.name]
    if not success_file:
        return 'no_file_exists'
    else:
        return 'load_csv_to_staging'  


def create_tasks(level1_dag, table_name):
    ''' Contains the list of tasks to be performed for each table load '''
    start = DummyOperator(task_id="start", dag = level1_dag)
    
    end = DummyOperator(task_id="end", trigger_rule="one_success", dag = level1_dag)
    
    no_file_exists = DummyOperator(task_id = "no_file_exists", dag = level1_dag)

    check_for_data_file = BranchPythonOperator(
        task_id = "check_for_data_file",
        python_callable= _branch,
        op_kwargs={"file_pattern":"{}".format(table_name)},
        dag = level1_dag
    )

    load_csv_to_staging = GCSToBigQueryOperator(
        task_id='load_csv_to_staging',
        bucket=SOURCE_BUCKET,
        source_objects=["{}/{}_*.csv".format(STAGING_FOLDER, table_name)],
        destination_project_dataset_table='{}.{}.{}_stg'.format(PROJECT_ID,STAGING_DATASET, table_name),
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        schema_object="schema_stg/{}_stg.json".format(table_name),        
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        dag = level1_dag
    )

    load_target_table = BigQueryInsertJobOperator(
        task_id="load_target_table",
        configuration={
            "query":{
                "query":query.replace("$sql_script",table_name),
                "useLegacySql":False
            }
        },
        params=params,
        dag=level1_dag
    )    

    move_files_to_archive =  GCSToGCSOperator(
        task_id="move_files_to_archive",
        move_object=True, 
        source_bucket=SOURCE_BUCKET,
        destination_bucket=SOURCE_BUCKET,
        source_object="{}/{}".format(STAGING_FOLDER, table_name),
        destination_object="{}/".format(ARCHIVE_FOLDER),
        dag = level1_dag        
    )


    start >> check_for_data_file >> [load_csv_to_staging, no_file_exists]
    load_csv_to_staging >> load_target_table >> move_files_to_archive >> end
    no_file_exists >> end        

#Top DAG
dag = DAG(
    dag_id=MAIN_DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    template_searchpath=TEMPLATE_SEARCHPATH
)

with dag:
    # Top DAG Initial Task
    start = DummyOperator(task_id="start")

    daily_load_tables_list = DATA_IMPORT_TABLES_LIST.split(',')
    level1_subdag_operators  = []

    for table in daily_load_tables_list:

        level1_dag = create_sub_dag(
            MAIN_DAG_ID
            ,table
            ,datetime(2022,11,22)
            ,'@once'
            ,TEMPLATE_SEARCHPATH
        )

        level1_subdag_operator = SubDagOperator(
            subdag = level1_dag,
            task_id = table,
            priority_weight = 1,
            weight_rule=WeightRule.ABSOLUTE,
            dag = dag
        )

        level1_subdag_operators.append(level1_subdag_operator)

        create_tasks(level1_dag, table)

        level1_dag = None

    start >> level1_subdag_operators




