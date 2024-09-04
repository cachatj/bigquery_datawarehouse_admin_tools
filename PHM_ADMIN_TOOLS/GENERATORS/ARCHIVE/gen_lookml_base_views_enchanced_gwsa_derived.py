from google.cloud import bigquery
import os


# Initialize BigQuery client
client = bigquery.Client()


# SQL Query
sql = """
SELECT DISTINCT
    vw_entity_name,
    INITCAP(REPLACE(vw_table_name_cleaned,'_',' ')) AS looker_view_label,
    vw_ordinal_postition,
    looker_base_view_path,
    vw_schema,
    looker_refinement_view_name,
    looker.vw_column_name,
   -- looker_label,
    CASE
      WHEN vw_schema  LIKE '%pr_sales_cube%' THEN INITCAP(REPLACE(looker.vw_column_name,"_"," "))
      WHEN vw_schema  LIKE '%d1_phm_dw_log%' THEN INITCAP(REPLACE(looker.vw_column_name,"_"," "))
      ELSE looker_label
    END AS looker_label,
    REPLACE(REPLACE(REPLACE(looker_description,'"','\\"'),'"',""),'�',' ') AS looker_description,
    looker_data_type, 
    looker_sql_table_name_param,
    CASE
      WHEN looker_data_type = 'number' AND REGEXP_CONTAINS(looker.vw_column_name, r"_qty$") THEN 'qty'
      WHEN looker_data_type = 'number' AND REGEXP_CONTAINS(looker.vw_column_name, r"_amt$") THEN 'amt'
      WHEN looker_data_type = 'number' AND REGEXP_CONTAINS(looker.vw_column_name, r"_dlr$") THEN 'amt'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_id$") THEN 'id'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_num$") THEN 'num'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_cd$") THEN 'cd'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_code$") THEN 'cd'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_flg$") THEN 'flag'
      WHEN looker_data_type = 'string' AND REGEXP_CONTAINS(looker.vw_column_name, r"_flag$") THEN 'flag'
      WHEN REGEXP_CONTAINS(looker.vw_column_name, r"_dte$") THEN 'date'
      WHEN REGEXP_CONTAINS(looker.vw_column_name, r"_date$") THEN 'date'
      ELSE NULL
    END AS column_classification,
    CASE 
      WHEN LOWER(vw_schema) LIKE '%gwsa%' THEN 'pharma_gwsa_looker'
      ELSE 'pharma-data-warehouse-looker'
    END AS git_repo_directory,
    CASE WHEN grants.required_access_grants IS NULL THEN "gwsaoperational" ELSE grants.required_access_grants  END AS required_access_grants,
FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V` looker
 LEFT JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_ACCESS_GRANTS_V` grants
  ON grants.ADMIN_ENTITY_INVENTORY_ENTITY_SCHEMA = UPPER(looker.vw_schema)
  AND grants.ADMIN_ENTITY_INVENTORY_ENTITY_NAME = UPPER(looker.vw_entity_name)
  AND grants.VW_COLUMN_NAME = UPPER(looker.vw_column_name)
WHERE 1=1
  AND looker_label IS NOT NULL
  AND vw_schema LIKE '%gwsa%'
  AND vw_schema NOT LIKE '%pd_oe_conv%'
ORDER BY vw_schema,vw_entity_name,vw_ordinal_postition ASC
"""


# Run the query and store results in a list
query_job = client.query(sql)
results = list(query_job.result())


# Dictionary to store LookML content for each vw_entity_name
lookml_dict = {}


# Dictionary to store directory for each vw_entity_name
directory_dict = {}


# Iterate through the results
for row in results:
    directory = os.path.join(row.git_repo_directory.lower(), 'views', 'derived_views', row.vw_schema.lower())
    os.makedirs(directory, exist_ok=True)
    directory_dict[row.vw_entity_name] = directory

    if row.vw_entity_name not in lookml_dict:
        lookml_dict[row.vw_entity_name] = f"""
  view: {row.vw_entity_name} {{
    derived_table: {{
      persist_for: "8 hours"
      sql: SELECT * FROM {row.looker_sql_table_name_param} ;;
    }}
  """
        
    # Check the data type and generate the appropriate LookML code
    if row.looker_data_type == 'date':
        lookml_field = f"""
  dimension_group: {row.vw_column_name} {{
    type: time
    label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    description: "{row.looker_description}"
    timeframes: [raw, date, week, month, quarter, year,fiscal_month_num,fiscal_quarter,fiscal_quarter_of_year,fiscal_year,day_of_week,day_of_month,day_of_year,month_name,month_num,quarter_of_year,week_of_year]
    datatype: date
    convert_tz: no
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""
    elif row.looker_data_type == 'time':
        lookml_field = f"""
  dimension_group: {row.vw_column_name} {{
    type: time
    label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    description: "{row.looker_description}"
    timeframes: [raw, date, week, month, quarter, year,fiscal_month_num,fiscal_quarter,fiscal_quarter_of_year,fiscal_year,day_of_week,day_of_month,day_of_year,month_name,month_num,quarter_of_year,week_of_year]
    convert_tz: no
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""
  # Generate Currency Formatted      
    elif row.column_classification == 'amt':
        lookml_field = f"""

  dimension: {row.vw_column_name} {{
    type: number
    label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    hidden: no
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}

  measure: {row.vw_column_name}_sum {{
    type: sum
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Sum"
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_average {{
    type: average
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Average"
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}    
  measure: {row.vw_column_name}_min {{
    type: min
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Min"
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_max {{
    type: max
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Max"
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_distinct {{
    type: count_distinct 
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Distinct"
    description: "{row.looker_description}"
    value_format: "$#,##0.00;($#,##0.00)"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""
  # Generate Quantity Formatted
    elif row.column_classification == 'qty':
        lookml_field = f"""
  dimension: {row.vw_column_name} {{
    type: number
    label: "{row.looker_label} (Being Deprecated - Use New Measure Group)"
    description: "{row.looker_description}"
    required_access_grants: [{row.required_access_grants}]
    hidden: yes
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}

  measure: {row.vw_column_name}_sum {{
    type: sum
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Sum"
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_average {{
    type: average
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Average"
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
        
  measure: {row.vw_column_name}_min {{
    type: min
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Min"
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_max {{
    type: max
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Max"
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}
  measure: {row.vw_column_name}_distinct {{
    type: count_distinct 
    group_label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    label: "{row.looker_label} Distinct"
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""

    else:
        lookml_field = f"""
  dimension: {row.vw_column_name} {{
    type: {row.looker_data_type}
    label: "{row.looker_label}"
    required_access_grants: [{row.required_access_grants}]
    description: "{row.looker_description}"
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""
    lookml_dict[row.vw_entity_name] += lookml_field


# Finalize the LookML content and save the files
for vw_entity_name, lookml_content in lookml_dict.items():
    lookml_content += """
  measure: count {
    type: count
  }
}
    """
    directory = directory_dict.get(vw_entity_name, '')
    filename = os.path.join(directory, f"{vw_entity_name}.view.lkml")
    with open(filename, 'w') as file:
        file.write(lookml_content)


print("LookML files generated successfully.")