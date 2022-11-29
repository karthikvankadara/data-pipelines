from google.colab import auth
from google.cloud import bigquery, storage

PROJECT_NAME = "PROJECT_NAME"
BUCKET_NAME = "BUCKET_NAME"

# authenticating Colab client
auth.authenticate_user()
client = bigquery.Client(PROJECT_NAME)
storage_client = storage.Client(PROJECT_NAME)
bucket = storage_client.get_bucket(BUCKET_NAME)

#query from the bigquery_array_struct_example.sql
sql_get_data = "select *"

query_job = client.query(sql_get_data)
results = query_job.result()
df = query_job.to_dataframe()

#perform formatting
json_obj = df.to_json(orient='records', indent=4)
blob = bucket.blob("output/marvel_character_info.json")
blob.upload_from_string(json_obj, content_type="application/json")

