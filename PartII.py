# It's convenient to have a local sample of owners to do work. 
# This notebook generates a file containing a sample of 
# owners and all of their transactions.

import numpy as np
import pandas as pd
from google.cloud import bigquery

# Connect to GoogleBQ
client = bigquery.Client(project="engellantwedge2024")

# Gather a list of all owners
owners_query = '''
    SELECT DISTINCT(card_no) AS dist_card_no
    FROM `engellantwedge2024.transactions.transArchive_*`
    WHERE card_no != 3
'''

owners_query_job = client.query(owners_query)
owners_query_results = owners_query_job.result()
dist_owners = owners_query_results.to_dataframe()

# Export df to data folder as sample_of_owners_full.csv
dist_owners.to_csv('data/sample_of_owners_list.csv', index=False)


# Read in the owners
owners = pd.read_csv('data/sample_of_owners_list.csv')

# len(owners['dist_card_no']) # 27207 owners
# Randomly select 400 of the owners
np.random.seed(123)
owners_sample = list(owners.sample(n=400)['dist_card_no'])

owners_tuple_str = ', '.join([str(owner) for owner in owners_sample])

sample_query = f'''
    SELECT *
    FROM `engellantwedge2024.transactions.transArchive_*`
    WHERE card_no IN ({owners_tuple_str})
'''

sample_query_job = client.query(sample_query)
sample_query_results = sample_query_job.result()
sample_df = sample_query_results.to_dataframe()

memory_in_bytes = sample_df.memory_usage(deep=True).sum()
memory_in_mb = memory_in_bytes / (1024 ** 2)  # Convert bytes to MB

# Print the size of the sample df
print(f"Memory usage: {memory_in_mb:.2f} MB") 


# Write the sample of owners to a '|' seperated csv file called sample_of_owners.csv
sample_df.to_csv('data/sample_of_owners.csv', index = False, sep = '|')
