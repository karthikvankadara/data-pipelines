import pyspark.pandas as ps
import pandas as pd

# file containing list of tables for which schema information is to be searched / collected
path = "dbfs:/FileStore/schema_extract_table_names_list.csv"

# read the file info and store it in the dataframe
tables_df = spark.read.csv(path ,header=True)

# get the value in form of a list
tables = tables_df.selectExpr("collect_list(table_names) as tables").first().tables

# variable to store results
output = []

for table in tables:
    dataset_name, table_name = table.split('.')

    # query to get schema information
    sql_get_table_info = "DESCRIBE {0}.{1}".format(dataset_name, table_name)

    try:
        columns_df = ps.sql(sql_get_table_info).to_pandas()
    except:
        continue
    
    #loop throw all the columns values and get the name and data type
    for _,row in columns_df.iterrows():
        column_name = row.col_name
        data_type = row.data_type

        output.append((dataset_name, table_name, column_name, data_type))

df = pd.DataFrame(output, columns=['dataset_name','table_name','column_name','data_type'])


##scenario 1 - if you need to search for particular column in the result set

# column name to search through
col_name_to_search = 'col_name'

# need for further processing
if col_name_to_search in df['column_name'].values():
    print("Column name {0} is present in following tables:".format(col_name_to_search))
    display(df[df['column_name'] == col_name_to_search])
else:
    print("Column name {0} not present in the given datasets.".format(col_name_to_search))

##scenario 2 - if you need to export the data

#variable to hold dbfs file path to store output
path = "/dbfs/path-to-output.csv"
df.write.csv(path, header=True)