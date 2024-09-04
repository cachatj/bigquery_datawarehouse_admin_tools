from google.cloud import bigquery
import os


# Initialize BigQuery client
client = bigquery.Client()


# Your SQL query
sql = """
SELECT DISTINCT
    vw_entity_name,
    INITCAP(REPLACE(vw_table_name_cleaned,'_',' ')) AS looker_view_label,
    vw_ordinal_postition,
    looker_base_view_path,
    vw_schema,
    looker_refinement_view_name,
    vw_column_name,
    looker_label,
    REPLACE(looker_description,'"','\\"') AS looker_description,
    CASE 
      WHEN LOWER(vw_schema) LIKE '%gwsa%' THEN 'pharma_gwsa_looker'
      ELSE 'pharma-data-warehouse-looker'
    END AS git_repo_directory,
    entity.days_since_create_date,
FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V` looker
 JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T` entity
   ON looker.vw_entity_name = LOWER(entity.entity_name)
   AND looker.vw_schema = LOWER(entity.entity_schema)
 WHERE 1=1 
  AND looker_label IS NOT NULL
  --AND entity.days_since_create_date < 10
  AND vw_schema NOT LIKE '%pd_oe_conv%'
ORDER BY vw_schema,vw_entity_name,vw_ordinal_postition ASC
"""


# Run the query and store results in a list
query_job = client.query(sql)
results = list(query_job.result())


# Dictionary to store LookML content for each vw_entity_name
lookml_dict = {}


# Iterate through the results
for row in results:
    # Directory based on vw_schema inside 'refinement_views'
    directory = os.path.join(row.git_repo_directory.lower(),'views','refinement_views', row.vw_schema)
    os.makedirs(directory, exist_ok=True) # Create folder if not exists
    
    # Start building the LookML content if not already started
    if row.vw_entity_name not in lookml_dict:
        lookml_dict[row.vw_entity_name] = f"""
include: {row.looker_base_view_path}


view: {row.looker_refinement_view_name} {{
  view_label: "{row.looker_view_label}"

"""
    
    # Append the dimension for the current vw_column_name
#     lookml_dict[row.vw_entity_name] += f"""
#   dimension: {row.vw_column_name} {{
#     label: "{row.looker_label}"
#     description: "{row.looker_description}"
#     hidden: no
#   }}
# """


# Finalize the LookML content and save the files
for vw_entity_name, lookml_content in lookml_dict.items():
    lookml_content += """
  #dimension: your_primary_key {
     #label: "View Primary Key"
     #description: "Add your primary key to the refinement view"
     #primary_key:yes
     #hidden: no  
  #}
  measure: count {
    type: count
  }
}
    """
    
    # Determine the appropriate directory and filename using the stored list
    for row in results:
        if row.vw_entity_name == vw_entity_name:
            directory = os.path.join(row.git_repo_directory.lower(),'views','refinement_views', row.vw_schema.lower())
            filename = os.path.join(directory, f"{row.vw_entity_name}.view.lkml")
            break
    
    # Check if the file already exists
    if os.path.exists(filename):
        print(f"File {filename} already exists. Skipping...")
    else:
        # Save the file
        with open(filename, 'w') as file:
            file.write(lookml_content)
        print(f"File {filename} saved successfully.")


print("LookML files generated successfully.")