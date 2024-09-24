# This script connects to GoogleBigQuery and then builds several summary tables 
# that will be useful for the analysis of the Wedge project.

import pandas as pd
from google.cloud import bigquery
import sqlite3

# Set up GoogleBQ
client = bigquery.Client(project="umt-msba")

# Query 1 - 
# Sales by day by hour
query_1 = '''
    SELECT 
    EXTRACT(DATE FROM datetime) AS date
    , EXTRACT(HOUR FROM datetime) AS hour
    , SUM(total) AS sales
    , COUNT(DISTINCT CONCAT(
        CAST(EXTRACT(DATE FROM datetime) AS STRING),
        CAST(register_no AS STRING),
        CAST(emp_no AS STRING),
        CAST(trans_no AS STRING)
    )) AS transactions
    , SUM(CASE WHEN trans_status = "V" OR
        trans_status = "R"
        THEN -1 ELSE 1 END) AS items
    FROM `umt-msba.transactions.transArchive_*`
    WHERE department NOT IN (0, 15)
    AND (trans_status IS NULL
    OR trans_status IN (" ", "", "V", "R"))
    GROUP BY date, hour
    ORDER BY date ASC, hour ASC;'''
    
# Query 2 - 
# Sales by owner by month by year. columns: card_no, year, month, sales, transactions, and items.
query_2 = '''
    SELECT 
    card_no AS card_no
    , EXTRACT(YEAR FROM datetime) AS year
    , EXTRACT(MONTH FROM datetime) AS month
    , SUM(total) AS sales
    , COUNT(DISTINCT CONCAT(
        CAST(EXTRACT(DATE FROM datetime) AS STRING),
        CAST(register_no AS STRING),
        CAST(emp_no AS STRING),
        CAST(trans_no AS STRING)
    )) AS transactions
    , SUM(CASE WHEN trans_status = "V" OR
        trans_status = "R"
        THEN -1 ELSE 1 END) AS items
    FROM `umt-msba.transactions.transArchive_*`
    WHERE department NOT IN (0, 15)
    AND (trans_status IS NULL
    OR trans_status IN (" ", "", "V", "R")) 
    AND card_no != 3
    GROUP BY year, month, card_no
    ORDER BY year ASC, month ASC;'''

# Query 3 -
# Sales by product description by year by month: 
# A file that has the following columns: upc, description, department number, department name, 
#                                        year, month, sales, transactions, and items.
query_3 = '''
SELECT 
upc as upc
, description as description
, department as dept_num
, CASE department
    WHEN 1 THEN 'PACKAGED GROCERY'
    WHEN 2 THEN 'PRODUCE'
    WHEN 3 THEN 'BULK'
    WHEN 4 THEN 'REF GROCERY'
    WHEN 5 THEN 'CHEESE'
    WHEN 6 THEN 'FROZEN'
    WHEN 7 THEN 'BREAD'
    WHEN 8 THEN 'DELI'
    WHEN 9 THEN 'GEN MERCH'
    WHEN 10 THEN 'SUPPLEMENTS'
    WHEN 11 THEN 'PERSONAL CARE'
    WHEN 12 THEN 'HERBS&SPICES'
    WHEN 13 THEN 'MEAT'
    WHEN 14 THEN 'JUICE BAR'
    WHEN 15 THEN 'MISC P/I'
    WHEN 16 THEN 'FISH&SEAFOOD'
    WHEN 17 THEN 'BAKEHOUSE'
    WHEN 18 THEN 'FLOWERS'
    WHEN 19 THEN 'WEDGEWORLDWIDE'
    WHEN 20 THEN 'MISC P/I - WWW'
    WHEN 21 THEN 'CATERING'
    WHEN 22 THEN 'BEER & WINE'
    ELSE 'UNKNOWN'
  END AS dept_name
, EXTRACT(YEAR FROM datetime) AS year
, EXTRACT(MONTH FROM datetime) AS month
, SUM(total) AS sales
, COUNT(DISTINCT CONCAT(
    CAST(EXTRACT(DATE FROM datetime) AS STRING),
    CAST(register_no AS STRING),
    CAST(emp_no AS STRING),
    CAST(trans_no AS STRING)
)) AS transactions
, SUM(CASE WHEN trans_status = "V" OR
    trans_status = "R"
    THEN -1 ELSE 1 END) AS items
FROM `umt-msba.transactions.transArchive_*`
WHERE department NOT IN (0, 15)
    AND (trans_status IS NULL
    OR trans_status IN (" ", "", "V", "R"))
GROUP BY year, month, upc, description, department
ORDER BY year ASC, month ASC;'''

#Execute the queries
#Then - Get the results
#Then - Convert the results to a pandas dataframe
query_job1 = client.query(query_1)
results1 = query_job1.result()
sales_by_day_by_hour = results1.to_dataframe()

query_job2 = client.query(query_2)
results2 = query_job2.result()
sales_by_owner_by_month_by_year = results2.to_dataframe()

query_job3 = client.query(query_3)
results3 = query_job3.result()
sales_by_product_desc_by_year_by_month = results3.to_dataframe()





###### THIS NEXT SECTION BUILDS THE SQLITE DATABASE ######

# Create and connect to the sqlite database
conn = sqlite3.connect('wedge_summary.db')

# Write the dataframes to the sqlite database
sales_by_day_by_hour.to_sql('sales_by_day_by_hour', conn, if_exists='replace', index=False)
sales_by_owner_by_month_by_year.to_sql('sales_by_owner_by_month_by_year', conn, if_exists='replace', index=False)
sales_by_product_desc_by_year_by_month.to_sql('sales_by_product_desc_by_year_by_month', conn, if_exists='replace', index=False)

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Database created successfully!")



# NEEDS WORK -
# This looks close but the sales columns for the second and 
# third tables are not correct.