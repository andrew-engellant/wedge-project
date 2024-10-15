from google.cloud import bigquery

# Initialize a BigQuery client
client = bigquery.Client()

# Set your project ID and dataset
project_id = 'engellantwedge2024'
dataset_id = 'wedge_transactions'

# Query to get all table names in the specified dataset
tables_query = f"""
    SELECT COUNTIF(trans_subtype is NULL) as Null_TS,
    COUNTIF(datetime is NULL) as Null_DT,
    COUNTIF(local IS NULL) as Null_Local,
    COUNTIF(card_no IS NULL) as Null_CN
    FROM `engellantwedge2024.wedge_transactions.transArchive_*`
"""

# Execute the query to retrieve table names
results = client.query(tables_query).result()

df = results.to_dataframe()

# Loop through each table and check for problematic values in the 'local' column
for table in tables:
    table_name = table.table_name
    query = f"""
        SELECT 
            local AS problematic_local_value
        FROM 
            `{project_id}.{dataset_id}.{table_name}`
        WHERE 
            local IS NOT NULL AND 
            SAFE_CAST(local AS INT64) IS NULL
    """
    
    # Execute the query
    results = client.query(query).result()
    
    # Print out the results
    if results.total_rows > 0:
        print(f"Table: {table_name}")
        for row in results:
            print(row.problematic_local_value)

# Close the BigQuery client (optional, as it will close on script exit)
client.close()

for table in tables:
    table_name = table.table_name
    print(table_name)
    
    
    
#######################################
#######################################
#######################################



    
from google.cloud import bigquery

# Initialize a BigQuery client
client = bigquery.Client()

# Set your project ID and dataset
project_id = 'engellantwedge2024'
dataset_id = 'wedge_transactions'

# Query to get all table names in the specified dataset
tables_query = f"""
    SELECT table_name 
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
"""

# Execute the query to retrieve table names
tables = list(client.query(tables_query).result())  # Convert to a list

# Loop through each table and check for problematic values in the 'local' column
for table in tables:
    table_name = table.table_name
    print(f"Checking table: {table_name}")  # Print table name for reference
    query = f"""
    SELECT COUNTIF(trans_subtype is NULL) as Null_TS,
    COUNTIF(datetime is NULL) as Null_DT,
    COUNTIF(local IS NULL) as Null_Local,
    COUNTIF(card_no IS NULL) as Null_CN
    FROM `{project_id}.{dataset_id}.{table_name}`
"""
  
    # Execute the query
    try:
        results = client.query(query).result()
        df = results.to_dataframe()
    except Exception as e:
        print(f"Error querying table {table_name}: {e}")
        


# Close the BigQuery client (optional, as it will close on script exit)
client.close()


#######################################
#######################################
#######################################

from google.cloud import bigquery
import re

# Initialize the BigQuery client
client = bigquery.Client()

# Set the project and dataset
project_id = "engellantwedge2024"
dataset_id = "wedge_transactions"

# Get the list of tables in the dataset
dataset_ref = client.dataset(dataset_id, project=project_id)
tables = list(client.list_tables(dataset_ref))

# Regular expression pattern to match tables starting with "transArchive_"
pattern = re.compile(r'^transArchive_')

# Iterate through the tables
for table in tables:
    table_id = table.table_id

    # Check if the table name starts with "transArchive_"
    if pattern.match(table_id):
        # Get the table schema
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)

        # Find the 'local' column and print its data type
        for field in table.schema:
            if field.name == 'local':
                print(f"Table: {table_id}, Column 'local' data type: {field.field_type}")
                break
        else:
            print(f"Table: {table_id}, Column 'local' not found")
    else:
        print(f"Skipping table: {table_id} (does not match 'transArchive_' pattern)")

# Print a summary
print(f"\nTotal tables processed: {len(tables)}")
print(f"Tables starting with 'transArchive_': {sum(1 for table in tables if pattern.match(table.table_id))}")

#######################################
#######################################
#######################################