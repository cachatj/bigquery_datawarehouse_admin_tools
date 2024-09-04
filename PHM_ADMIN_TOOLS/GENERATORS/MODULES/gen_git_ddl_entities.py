import os
import re
import sqlparse
from google.cloud import bigquery
from datetime import datetime
import git_operations

# Define your repository path and branch name
repo_path = 'pharma-data-warehouse-edna/'
branch_name = 'admin_gen_ddl'

# Call the function
git_operations.switch_to_branch(repo_path, branch_name)


# Get current date
current_date = datetime.now().strftime('%m%d%Y')


# Instantiate a BigQuery client
bq = bigquery.Client()


# Query to fetch all the entities and their respective DDLS
query = """
SELECT 
    entity_catalog,
    schema_name_group,
    entity_schema,
    entity_name,
    entity_type,
    create_date,
    entity_ddl,
FROM  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T`
WHERE entity_name NOT LIKE '%_TEST'
AND entity_type <> 'SNAPSHOT'
"""


# Fetch the data
data = bq.query(query).to_dataframe()


# Iterate over each row
for _, row in data.iterrows():
    # Create directories for each dataset and table type if they do not exist
    dir_name = os.path.join(f'pharma-data-warehouse-edna/',row['schema_name_group'], row['entity_schema'], row['entity_type'])


    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


    # Create the full path for the file
    file_path = os.path.join(dir_name, f"{row['entity_name']}.sql")


    # Write the DDL to a .sql file
    with open(file_path, 'w', encoding='utf-8') as f:
        # Remove consecutive newlines
        #cleaned_ddl = re.sub("\r?\n\s*\r?\n*", "\n", row['ddl'])
        # Format the SQL using sqlparse
        #formatted_sql = sqlparse.format(row['ddl'], reindent=True, keyword_case='upper')
        temp_formatted_sql = sqlparse.format( row['entity_ddl'], reindent=False, keyword_case='upper', identifier_case=None, strip_comments=False, reindent_aligned=False, indent_tabs=False, indent_width=4 )
        # Remove newlines 2.0
        #formatted_sql = re.sub("\n\s*\n","\n", temp_formatted_sql)
        formatted_sql = re.sub("\n{2,}","\n\n", temp_formatted_sql)
        f.write(formatted_sql)

print("DDL Backup and Git operations completed successfully.")