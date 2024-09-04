import pandas as pd
import datetime


def generate_sql_procedure(entity_name, entity_schema, group):
    # Get the current date
    creation_date = datetime.date.today().strftime('%m/%d/%Y')
   
    # Initialize the SQL statement
    sql = f"""
    CREATE PROCEDURE  `PROJECT_ID.{entity_schema}.SP_{entity_name}_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: Joey Holmesmeyer
    Creation Date: {creation_date}
    Sproc Purpose: Delta Loading Merge Sproc for {entity_name}
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
    SET v_stored_proc_name = '{entity_schema}.{entity_name}_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.{entity_schema}.{entity_name}`);


    MERGE INTO  `PROJECT_ID.{entity_schema}.{entity_name}` AS target
     USING 
      (
        -- Developer's code goes here (SAMPLE CODE BELOW)
         --SELECT
         --FROM 
         --`PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_SDW__INVOICE_LINE_CV` il
        --JOIN
         --`PROJECT_ID.VI2_PHM_CONFDIM_TIME.PHM_SDW__TIME_DETAIL_CV` t
           --ON il.DTE_KEY_NUM = t.DTE_KEY_NUM
            --WHERE il.D0_UPDATE_STP > DATE_SUB(last_load_timestamp, INTERVAL 5 DAY) --- MOST IMPORTANT CONCEPT
            --AND DIST_CENTER_KEY_NUM IN (128,162)
      ) AS source\n"""

    # Add the ON part
    sql += "      ON TARGET.xyz = SOURCE.xyz\n"


    # Add the WHEN MATCHED THEN UPDATE SET part
    sql += "    WHEN MATCHED THEN\n      UPDATE SET\n"
    for index, row in group.iterrows():
        sql += f"        TARGET.{row['Column Name']} = SOURCE.{row['Column Name']},\n"

    # Remove the last comma
    sql = sql[:-2] + "\n"

    # Add the WHEN NOT MATCHED THEN INSERT VALUES part
    sql += "    WHEN NOT MATCHED THEN\n      INSERT VALUES\n      (\n"
    for index, row in group.iterrows():
        sql += f"        SOURCE.{row['Column Name']},\n"

    # Remove the last comma and add the END part
    sql = sql[:-2] + "\n      );\n    END;\n"

    return sql

# Read the CSV file
df = pd.read_csv('pharma-data-warehouse-edna/PHM_ADMIN_TOOLS/INPUT_FILES/sproc_merge.csv')

# Group the DataFrame by 'Entity Name'
groups = df.groupby('Entity Name')

for name, group in groups:
    entity_schema = group['Entity Schema'].iloc[0]
    sql_procedure = generate_sql_procedure(name, entity_schema, group)


    # Write the SQL procedure to a file
    with open(f'pharma-data-warehouse-edna/PHM_ADMIN_TOOLS/OUTPUT_FILES/SP_{name}_DELTA_LOAD.sql', 'w') as f:

        f.write(sql_procedure)