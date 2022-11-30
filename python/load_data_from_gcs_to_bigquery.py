import pandas as pd
from google.cloud import storage,bigquery
import io

PROJECT_ID = "PROJECT_ID"
SOURCE_BUCKET_NAME = "SOURCE_BUCKET_NAME"

sql_get_schema=r"""
select
    STRING_AGG(column_name,',') as column_names
    ,STRING_AGG(data_type,',') as data_types
from {0}.data_import.INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = '{1}_stg'
"""

def get_column_names_data_types(table_name):
    '''Gets the column_names and data_types for given table in two seperate lists'''
    client = bigquery.Client(PROJECT_ID)
    query_job = client.query(sql_get_schema.format(PROJECT_ID, table_name))
    results = query_job.result()
    output = []
    for row in results:
        output.append(row['column_name'].split(','))
        output.append(row['data_types'].split(','))
    return output

def create_schema(table_name):
    '''Obtain the schema of the table in bigquery schema format'''
    column_names, data_types = get_column_names_data_types(table_name)
    schema_list = []
    for column_name, data_type in zip(column_names, data_types):
        schema = bigquery.SchemaField(column_name, data_type)
        schema_list.append(schema)

    return schema_list

def load_data_to_staging(project_id, table_name):
    '''loads the data to staging table using files stored on gcs'''
    storage_client = storage.Client(project_id)
    bigquery_client = bigquery.Client(project_id)
    prefix = "staging/{0}".format(table_name)
    table_id = "{0}.data_import.{1}_stg".format(project_id, table_name)
    schema = create_schema(table_name)

    blobs = storage_client.list_blobs(SOURCE_BUCKET_NAME, prefix = prefix, delimiter=None)

    for blob in blobs:
        print("Loading records from {0}".format(blob.name))
        data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(data))
        
        job_config = bigquery.LoadJobConfig(schema=schema)
        job = bigquery_client.load_from_dataframe(
            df, table_id, job_config = job_config
        )
        job.result() # wait for job to complete
        table = bigquery_client.get_table(table_id) #Make an API request
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )