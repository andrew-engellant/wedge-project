import os
import zipfile
import pandas as pd
import csv
from google.cloud import bigquery
import pandas_gbq
import numpy as np


ddir = '/Users/drewengellant/Documents/MSBA/Fall24/ADA/wedge/wedge-project/data/WedgeZipOfZips/extracted/' #Path to small extracted tables

schema = [
    bigquery.SchemaField("datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("register_no", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("emp_no", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("trans_no", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("upc", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trans_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trans_subtype", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("trans_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("department", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("quantity", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Scale", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("unitPrice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("regPrice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("altPrice", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("tax", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("taxexempt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("foodstamp", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("wicable", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("discount", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("memDiscount", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("discountable", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("discounttype", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("voided", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("percentDiscount", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ItemQtty", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("volDiscType", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("VolSpecial", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("mixMatch", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("matched", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("memType", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("staff", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("numflag", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("itemstatus", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("tenderstatus", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("charflag", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("varflag", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("batchHeaderID", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("local", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("organic", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("display", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("receipt", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("card_no", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("store", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("branch", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("match_id", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("trans_id", "FLOAT", mode="NULLABLE"),
]

col_names = ['datetime', 'register_no', 'emp_no', 'trans_no', 'upc', 'description',
       'trans_type', 'trans_subtype', 'trans_status', 'department', 'quantity',
       'Scale', 'cost', 'unitPrice', 'total', 'regPrice', 'altPrice', 'tax',
       'taxexempt', 'foodstamp', 'wicable', 'discount', 'memDiscount',
       'discountable', 'discounttype', 'voided', 'percentDiscount', 'ItemQtty',
       'volDiscType', 'volume', 'VolSpecial', 'mixMatch', 'matched', 'memType',
       'staff', 'numflag', 'itemstatus', 'tenderstatus', 'charflag', 'varflag',
       'batchHeaderID', 'local', 'organic', 'display', 'receipt', 'card_no',
       'store', 'branch', 'match_id', 'trans_id']

# Function to analyze the CSV file using csv.Sniffer
def sniff_csv(file_path):
    try:
        with open(file_path, 'r', newline='') as csvfile:
            sample = csvfile.read(1024 * 4)
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(sample)
            dialect = sniffer.sniff(sample)
        print(f"Delimiter: {dialect.delimiter}, Has Header: {has_header}")
        return dialect.delimiter, has_header
    except Exception as e:
        print(f"Error sniffing CSV file {file_path}: {e}")
        raise

# Function to cast a DataFrame to a specified schema
def cast_dataframe_to_schema(df, schema):
    print(f"Casting DataFrame to BigQuery schema")
    try:
        for field in schema:
            column_name = field.name
            field_type = field.field_type

            if column_name in df.columns:
                if field_type == "STRING":
                    df[column_name] = df[column_name].astype(str)
                elif field_type == "FLOAT":
                    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')  # Coerce invalid values to NaN
                elif field_type == "BOOLEAN":
                    df[column_name] = df[column_name].apply(lambda x: bool(x) if pd.notnull(x) else None)  # Handle nulls
                elif field_type == "TIMESTAMP":
                    df[column_name] = pd.to_datetime(df[column_name], errors='coerce', utc=True)  # Convert to datetime and coerce errors

        df = df[[field.name for field in schema if field.name in df.columns]]
        return df
    except Exception as e:
        print(f"Error casting DataFrame to schema: {e}")
        raise

# Set up the BigQuery client
client = bigquery.Client(project='engellantwedge2024')

# Loop through the files in the directory
# Sniff files then load into pd dataframe
# Replace occurrences of '/N' and '//N' with 'NULL'
# Change cast datatypes to match schema
# Load file to the cloud
for file in os.listdir(ddir):
    if file.endswith('.csv'):
        print(f"\nProcessing file: {file}")

        ##### LOAD DATA #####
        try:
            file_path = os.path.join(ddir, file)
            delimiter, has_header = sniff_csv(file_path)
            df = pd.read_csv(file_path, delimiter=delimiter, header=0 if has_header else None, low_memory=False)
            print(f"Loaded data from {file}, shape: {df.shape}")
            
            # If no headers, assign column names
            if not has_header:
                df.columns = col_names
                print(f"Assigned column names for {file}")
        except Exception as e:
            print(f"Error loading data from {file}: {e}")
            continue

        ##### CLEAN DATA #####
        try:
            df = df.replace({'/N': 'NULL', '//N': 'NULL', ' ': 'NULL', '': 'NULL', np.nan: 'NULL', 'NaN': 'NULL', 'N': 'NULL'})
            print(f"Cleaned data for {file}")
        except Exception as e:
            print(f"Error cleaning data for {file}: {e}")
            continue

        ##### CAST DATA TYPES #####
        try:
            df = cast_dataframe_to_schema(df, schema)
            print(f"Casted data types for {file}")
        except Exception as e:
            print(f"Error casting data for {file}: {e}")
            continue
        
        ##### LOAD DATA TO BIGQUERY #####
        try:
            table_id = 'wedge_transactions.' + file.split('.')[0]
            print(f"Uploading {file} to BigQuery table: {table_id}")
            pandas_gbq.to_gbq(df, table_id, project_id = 'engellantwedge2024', if_exists='replace')
            print(f"Successfully uploaded {file} to BigQuery")
        except Exception as e:
            print(f"Error uploading {file} to BigQuery: {e}")
            continue


