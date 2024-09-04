from google.cloud import bigquery
import os
import datetime
import git_operations

# Define your repository path and branch name
repo_path = 'pharma-data-warehouse-edna/'
branch_name = 'admin_gen_ddl'

# Call the function
git_operations.switch_to_branch(repo_path, branch_name)

# Initialize BigQuery client
client = bigquery.Client()


# Your SQL query
sql = """
SELECT DISTINCT
    schema_name_group,
    table_schema,
    table_name,
    'SPROC_TEMPLATES' AS sproc_type,
    column_name,
    description,
    data_type,
    ordinal_position,
FROM
  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS` 

  WHERE 1=1
  AND dataset_group IN ('D1','D2','D4')
  AND pdw_flag IS TRUE
  AND table_type LIKE '%TABLE%'
  --and table_name = 'BUYING_GROUP_CV'
  ORDER BY schema_name_group, table_schema, table_name, ordinal_position ASC
"""


# Run the query and store results in a list
query_job = client.query(sql)
results = list(query_job.result())


# Dictionary to metadata for content for each d1 table
sproc_dict = {}

# Iterate through the results
#for row in results:


def generate_sql_procedure(table_name, table_schema, group):
    # Get the current date
    creation_date = datetime.date.today().strftime('%m/%d/%Y')
   
    # Initialize the SQL statement
    sql = f"""
    CREATE PROCEDURE  `PROJECT_ID.{table_schema}.SP_{table_name}_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for {table_name}
    Data Sources:  See SQL Below

    ************************************************************************************************************************/
    DECLARE v_start_time timestamp;
    DECLARE v_start_stp timestamp;
    DECLARE v_end_stp timestamp;
    DECLARE v_stored_proc_name STRING;
    DECLARE last_load_timestamp timestamp;

    SET v_start_time = current_timestamp();
    SET v_start_stp = current_timestamp();
    SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
    SET v_stored_proc_name = '{table_schema}.{table_name}_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.{table_schema}.{table_name}`);


    MERGE INTO  `PROJECT_ID.{table_schema}.{table_name}` AS target
     USING 
      (
     -- Developer's code goes here (SAMPLE CODE BELOW)

         --SELECT 
            a.column_1
            a.column_2
         --FROM 
         -- `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_SDW__INVOICE_LINE_CV` il
         --JOIN
            --`PROJECT_ID.VI2_PHM_CONFDIM_TIME.PHM_SDW__TIME_DETAIL_CV` t
                --ON il.DTE_KEY_NUM = t.DTE_KEY_NUM
        --WHERE il.D0_UPDATE_STP > last_load_timestamp --- MOST IMPORTANT CONCEPT

      ) AS source\n"""

    # Add the ON part
    sql += "      ON TARGET.PRIMARY_KEY = SOURCE.PRIMARY_KEY\n"


   # Add the WHEN MATCHED THEN UPDATE SET part
    sql += "    WHEN MATCHED THEN\n      UPDATE SET\n"
    for row in group:  # Change this line
        sql += f"        TARGET.{row['column_name']} = SOURCE.{row['column_name']},\n"

    # Remove the last comma
    sql = sql[:-2] + "\n"

    # Add the WHEN NOT MATCHED THEN INSERT VALUES part
    sql += "    WHEN NOT MATCHED THEN\n      INSERT VALUES\n      (\n"
    for row in group:  # And this line
        sql += f"        SOURCE.{row['column_name']},\n"

    # Remove the last comma and add the END part
    sql = sql[:-2] + "\n      );\n    END;\n"

    return sql

# Group the results by 'table_name'
groups = {}
for row in results:
    table_name = row['table_name']
    if table_name not in groups:
        groups[table_name] = []
    groups[table_name].append(row)

for name, group in groups.items():
    table_schema = group[0]['table_schema']
    schema_name_group = group[0]['schema_name_group']
    sproc_type = group[0]['sproc_type']
    sql_procedure = generate_sql_procedure(name, table_schema, group)

    # Create the directory hierarchy
    directory = f'pharma-data-warehouse-edna/{schema_name_group}/{table_schema}/{sproc_type}'
    os.makedirs(directory, exist_ok=True)


    # Write the SQL procedure to a file in the specified directory
    with open(f'{directory}/SP_{name}_DELTA_LOAD.sql', 'w') as f:
        f.write(sql_procedure)

print("Process complete!")  # Add this line