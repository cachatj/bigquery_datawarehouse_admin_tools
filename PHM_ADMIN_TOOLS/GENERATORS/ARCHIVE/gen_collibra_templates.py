import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

# Get current date
current_date = datetime.now().strftime('%m%d%Y')

# Instantiate a BigQuery client
bq = bigquery.Client()

# Query to fetch data
query = """
SELECT * 
FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LINEAGE_COLLIBRA_V`
--WHERE TRGT_SCHEMA_NAME LIKE '%CUSTOMER%'
ORDER BY TRGT_SCHEMA_NAME, TRGT_TABLE_NAME ASC
"""

# Fetch the data
data = bq.query(query).to_dataframe()

# Group the data by 'TRGT_SCHEMA_NAME'
grouped = data.groupby('TRGT_SCHEMA_NAME')

# Iterate over each group
for name, group in grouped:
    # Create a directory for each 'TRGT_SCHEMA_NAME' if it does not exist
    dir_name = os.path.join('C:/Users/joseph.holmesmeyer/OneDrive - Cardinal Health/Data_Engineering/Collibra','Generated_Files', name)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # Create the full path for the file
    file_path = os.path.join(dir_name, f"{name}_{current_date}.xlsx")

    # Write the group data to an Excel file
    group.to_excel(file_path, index=False)

    # Print the path of the created file
    print(f"File created: {file_path}")

print("Data extraction completed successfully.")