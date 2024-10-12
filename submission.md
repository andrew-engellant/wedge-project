# Applied Data Analytics
## Wedge Project Summary

This project involved building a functional database to query and analyze large-scale transaction data from a cooperative grocery store, commonly referred to as "The Wedge." The project was completed using Python, Google BigQuery, Pandas, and SQLite. Over the course of the project, I gained experience in data engineering, ETL processes, and relational database design.

### Task 1: Data Extraction and Database Upload

The goal of Task 1 was to build a functional database by processing raw transaction data programmatically using the Python Client for Google BigQuery. This task was divided into two scripts:

- **`partI-1.ipynb`**: Unzips transaction files and stores them in the *data/WedgeZipOfZips/extracted* directory. Metadata from these files are aggregated and saved as `summary.csv` in the *data* folder.
  
- **`partI-2.py`**: Loads transaction files into a Pandas DataFrame, cleans the data (standardizing NULL values and enforcing proper data types), and uploads them to Google BigQuery.

### Task 2: Sampling and Data Subsetting

With over 85 million transactions and nearly 30 GB of data, a smaller sample was needed for efficient local storage and analysis. In Task 2, I created a subset of the data by selecting transactions from a random sample of store owners.

- **`partII.py`**: Queries all store owners using the Python Client for Google BigQuery and retrieves all transactions for a random subset of these owners. The sample data is saved as `sample_of_owners_full.csv`, which is under 500 MB.

### Task 3: Creating a Relational Database

The ultimate goal of the project was to organize the transaction data for business analysis. Task 3 involved creating a relational database using SQLite, with summary tables that provide useful insights into sales trends and patterns.

- **`partIII.py`**: Queries transaction data from Google BigQuery and stores the results as tables in a SQLite database. The script creates three summary tables in `wedge_summary.db`:
  - **`sales_by_hour_by_day`**: Contains columns for *date, hour, sales, transactions, items*.
  - **`sales_by_month_by_year`**: Contains columns for *card_no, year, month, sales, transactions, items*.
  - **`sales_by_product_desc_by_year_by_month`**: Contains columns for *upc, description, department number, department name, year, month, sales, transactions, items*.

## Query Comparison Results

The following table summarizes the query results from the Google BigQuery dataset compared with the professor's (John) results. The table includes relative differences where applicable.

| Query | Your Results | John's Results | Difference | Rel. Diff |
|---|---|---|---|---|
| Total Rows | 85,760,139 | 85,760,139 | 0 | 0% |
| January 2012 Rows | 1,070,907 | 1,070,907 | 0 | 0% |
| October 2012 Rows | 1,042,287 | 1,042,287 | 0 | 0% |
| Month with Fewest | 2 | 2 | Yes | NA |
| Num Rows in Month with Fewest | 6,556,770 | 6,556,770 | 0 | 0% |
| Month with Most | 5 | 5 | Yes | NA |
| Num Rows in Month with Most | 7,578,372 | 7,578,372 | 0 | 0% |
| Null_TS |  | 7,123,792 | | |
| Null_DT | | 0 | | |
| Null_Local | | 234,843 | | |
| Null_CN | | 0 | | |
| Num 5 on High Volume Cards | 14,987 | 14,987 | Yes | NA |
| Num Rows for Number 5 | 460,625 | 460,430 | 195 | 0.04% |
| Num Rows for 18,736 | 12,153 | 12,153 | 0 | 0% |
| Product with Most Rows | Banana Organic | Banana Organic | Yes | NA |
| Num Rows for that Product | 908,639 | 908,639 | 0 | 0% |
| Product with Fourth-Most Rows | Avocado Hass Organic | Avocado Hass Organic | Yes | NA |
| Num Rows for that Product | 456,771 | 456,771 | 0 | 0% |
| Num Single Record Products | 2,741 | 2,769 | -28 | -1.01% |
| Year with Highest Portion of Owner Rows | 2014 | 2014 | Yes | NA |
| Fraction of Rows from Owners in that Year | 0.7591 | 0.7591 | 0 | 0% |
| Year with Lowest Portion of Owner Rows | 2011 | 2011 | Yes | NA |
| Fraction of Rows from Owners in that Year | 0.7372 | 0.7372 | 0 | 0% |

## Reflections

This was a great project and really spiked my interest in data engineering. I enjoyed having a bigger project that lasted longer than the typical couple of hours that it takes to complete most assignments for your classes. It challenging to close your computer for the day and return to the project the next day and refresh your memory on where you left off, but this feels realistic to how work is normally accomplished at a company. I felt I was challenged keeping focus on the different levels of perspective on this project (current chunk of code, function of the script, overall goal of the project) but it was satisfying when I allowed myself to zoom out to that highest perspective and appreciate what I had accomplished. I think the most beenficial part of this project was that you didn't start us with notebooks outlining the steps we had to take to accomplish the task. I enjoyed having a blank script to begin each task and exploring different ways to accomplish the task without knowing if they would work. 
