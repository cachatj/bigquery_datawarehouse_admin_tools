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
    vw_column_name,
   -- looker_label,
    CASE
      WHEN vw_schema  LIKE '%pr_sales_cube%' THEN INITCAP(REPLACE(vw_column_name,"_"," "))
      WHEN vw_schema  LIKE '%d1_phm_dw_log%' THEN INITCAP(REPLACE(vw_column_name,"_"," "))
      ELSE looker_label
    END AS looker_label,
    REPLACE(REPLACE(REPLACE(looker_description,'"','\\"'),'"',""),'�',' ') AS looker_description,
    looker_data_type,
    looker_sql_table_name_param,
    CASE 
      WHEN LOWER(vw_schema) LIKE '%gwsa%' THEN 'pharma_gwsa_looker'
      ELSE 'pharma-data-warehouse-looker'
    END AS git_repo_directory,
FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V`
WHERE 1=1
  AND looker_label IS NOT NULL
  AND vw_schema NOT LIKE '%csl_rpt%'
  AND vw_schema NOT LIKE '%gwsa%'
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
    directory = os.path.join('Advrp-LookML-Code-Repository', 'views', 'base_views', row.vw_schema.lower())
    # Advrp-LookML-Code-Repository\views\base_views
    os.makedirs(directory, exist_ok=True)
    directory_dict[row.vw_entity_name] = directory

    if row.vw_entity_name not in lookml_dict:
        lookml_dict[row.vw_entity_name] = f"""
view: {row.vw_entity_name}_pdw {{
  sql_table_name: {row.looker_sql_table_name_param} ;;
"""
    # Check the data type and generate the appropriate LookML code
    if row.looker_data_type == 'date':
        lookml_field = f"""
  dimension_group: {row.vw_column_name} {{
    type: time
    label: "{row.looker_label}"
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
    description: "{row.looker_description}"
    timeframes: [raw, date, week, month, quarter, year,fiscal_month_num,fiscal_quarter,fiscal_quarter_of_year,fiscal_year,day_of_week,day_of_month,day_of_year,month_name,month_num,quarter_of_year,week_of_year]
    convert_tz: no
    sql: ${{TABLE}}.{row.vw_column_name.upper()} ;;
  }}"""
    else:
        lookml_field = f"""
  dimension: {row.vw_column_name} {{
    type: {row.looker_data_type}
    label: "{row.looker_label}"
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
    filename = os.path.join(directory, f"{vw_entity_name}_pdw.view.lkml")
    with open(filename, 'w') as file:
        file.write(lookml_content)


print("LookML files generated successfully.")