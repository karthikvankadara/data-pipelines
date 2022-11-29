# example of how to extract bq objects from BigQuery using Python
from google.cloud import bigquery
import os

#declare the constants
project_id = "PROJECT_ID"
datasets = ['DATASET_A','DATASET_B']
output_path = '/views/{0}'
file_path = '/views/{0}/{1}.sql'

#sql queries
sql_get_list_of_views =r"""
select table_name as view_name
from `{0}.{1}.INFORMATION_SCHEMA.VIEWS`
"""

sql_get_view_ddl = r"""
select ddl
from
(
    --header
    select '''CREATE OR REPLACE VIEW `{0}.{1}.{2}`\nAS\n''' as ddl
    union all
    select view_definition as ddl
    from `{0}.{1}.INFORMATION_SCHEMA.VIEWS`
    where table_name = '''{2}'''
)
"""

#create output directory if it does not exist
for dataset in datasets:
    if not os.path.exists(output_path.format(dataset)):
        os.makedirs(output_path.format(dataset))

client = bigquery.Client(project_id)

for dataset in datasets:
    #getting all the views for given dataset
    query_job = client.query(sql_get_list_of_views.format(project_id, dataset))
    results = query_job.result()
    #loop through each view definition for given dataset
    for row in results:
        view = row.view_name
        query_job_inr = client.query(sql_get_view_ddl.format(project_id, dataset, view))
        results_inr = query_job_inr.result()
        
        output = file_path.format(dataset, view)
        ddl = []

        for row in results_inr:
            ddl.append(row.ddl)

        f = open(file_path, 'w', newline='\n')
        f.write(''.join(ddl))
        f.close()
