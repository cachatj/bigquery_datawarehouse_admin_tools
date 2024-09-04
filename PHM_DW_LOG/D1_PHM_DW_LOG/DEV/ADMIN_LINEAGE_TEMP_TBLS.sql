    WITH

    entities AS 
    (
    SELECT
        ReferencedTables.dataset_id AS SRC_SCHEMA_NAME,
        ReferencedTables.table_id AS SRC_TABLE_NAME,
        admin_jobs_t.destination_table.dataset_id AS TRGT_SCHEMA_NAME,
        admin_jobs_t.destination_table.table_id AS TRGT_TABLE_NAME,
        CASE 
         WHEN ReferencedTables.dataset_id LIKE '%D0_%' THEN 1
         WHEN ReferencedTables.dataset_id LIKE '%VI2_%' THEN 2
         WHEN ReferencedTables.dataset_id LIKE '%D1_%' THEN 3
         ELSE 4
         END AS MATCH_RANK,
        MAX(creation_time) AS creation_time,

    FROM
        `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T` AS admin_jobs_t
        LEFT JOIN UNNEST(admin_jobs_t.referenced_tables) AS ReferencedTables -- Unnesting all tables referenced by sproc
    WHERE 1=1
        AND ReferencedTables.table_id IS NOT NULL
        AND admin_jobs_t.destination_table.dataset_id NOT LIKE '%VW_%'
        AND user_email LIKE '%sa-edna-pharma-data-whse%'
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        3 DESC
    ),

    temp_tbl AS(
    
    SELECT
        SRC_SCHEMA_NAME,
        SRC_TABLE_NAME,
        TRGT_SCHEMA_NAME,
        TRGT_TABLE_NAME,
        MAX(creation_time) AS creation_time,
    FROM entities 
    WHERE 1=1
        AND SRC_SCHEMA_NAME LIKE '%_script%'
        AND TRGT_SCHEMA_NAME NOT LIKE '%_script%'
        --AND TRGT_TABLE_NAME = 'BILLING_TRNSCT'
    GROUP BY 1,2,3,4
    ),

    temp_tbl_src AS(
    
    SELECT DISTINCT
        temp_tbl_src.SRC_SCHEMA_NAME,
        temp_tbl_src.SRC_TABLE_NAME,
        --temp_tbl_src.TRGT_SCHEMA_NAME,
        --temp_tbl_src.TRGT_TABLE_NAME,
        temp_tbl.TRGT_SCHEMA_NAME,
        temp_tbl.TRGT_TABLE_NAME,
        CASE 
         WHEN temp_tbl_src.SRC_SCHEMA_NAME LIKE '%D0_%' THEN 1
         WHEN temp_tbl_src.SRC_SCHEMA_NAME LIKE '%VI2_%' THEN 2
         WHEN temp_tbl_src.SRC_SCHEMA_NAME LIKE '%D1_%' THEN 3
         ELSE 4
         END AS MATCH_RANK,

        MAX(temp_tbl_src.creation_time) AS creation_time,
    FROM entities AS temp_tbl_src
        JOIN temp_tbl
        --ON temp_tbl.SRC_TABLE_NAME = temp_tbl_src.TRGT_TABLE_NAME
        ON temp_tbl.SRC_SCHEMA_NAME = temp_tbl_src.TRGT_SCHEMA_NAME
    WHERE 1=1
        AND temp_tbl_src.SRC_SCHEMA_NAME NOT LIKE '%_script%'
        AND temp_tbl_src.TRGT_SCHEMA_NAME  LIKE '%_script%'
        --AND temp_tbl_src.TRGT_TABLE_NAME = 'SUPPLIER_PARTNER_FUNCTION_CV'
    GROUP BY 1,2,3,4,5

    ),

    entities_union AS(
        SELECT * EXCEPT(creation_time) FROM entities 
         WHERE entities.TRGT_SCHEMA_NAME NOT LIKE '%script_%'
         AND entities.SRC_SCHEMA_NAME NOT LIKE '%script_%'
         --AND TRGT_TABLE_NAME = 'BILLING_TRNSCT'
        UNION DISTINCT

        SELECT * EXCEPT(creation_time) FROM temp_tbl_src
        

    )
,

    --select * from entities_union

    --- Extract the Source to Target Mapping from Stored Proc Runs (Column Level)
logic
AS(

SELECT DISTINCT
  SRC_SCHEMA_NAME,
  SRC_TABLE_NAME,
  columns_src.column_name AS SRC_COLUMN_NAME,
  columns_src.ordinal_position AS SRC_COLUMN_ORDINAL_POSITION,
  columns_target.column_name AS TRGT_COLUMN_NAME,
  columns_target.ordinal_position AS TRGT_COLUMN_ORDINAL_POSITION,
  columns_target.description AS TRGT_COLUMN_TECH_DESC,
  TRGT_SCHEMA_NAME,
  TRGT_TABLE_NAME,
  --- Ranking Logic to Determine the Highest Quality Column Match
  CASE
    WHEN SRC_TABLE_NAME = TRGT_TABLE_NAME THEN 98
    WHEN columns_src.column_name = columns_target.column_name THEN 1
    WHEN CONCAT(REPLACE(columns_src.column_name,"_CV",""),'_',SRC_TABLE_NAME) LIKE  CONCAT('%',columns_target.column_name, '%') THEN 2
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND  columns_target.column_name LIKE CONCAT('%',SRC_TABLE_NAME, '%') THEN 3
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND REGEXP_CONTAINS(REPLACE(SRC_TABLE_NAME, '__', '_'), CONCAT('_', REGEXP_EXTRACT(columns_target.column_name, r'_(\w+)$'))) IS TRUE THEN 4
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') THEN 5
    ELSE 99 
  END AS MATCH_RANK_ID,
  --- Ranking Logic to Determine the Highest Quality Column Match (Sort Order Applied)
  ROW_NUMBER() OVER (
    PARTITION BY TRGT_SCHEMA_NAME,
                 TRGT_TABLE_NAME,
                 columns_target.column_name
    ORDER BY 
     CASE
      WHEN SRC_TABLE_NAME = TRGT_TABLE_NAME THEN CONCAT('98-', CAST(entities_union.MATCH_RANK AS STRING))
      WHEN columns_src.column_name = columns_target.column_name THEN 1
      WHEN CONCAT(REPLACE(columns_src.column_name,"_CV",""),'_',SRC_TABLE_NAME) LIKE  CONCAT('%',columns_target.column_name, '%') THEN 2
      WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
       AND  columns_target.column_name LIKE CONCAT('%',SRC_TABLE_NAME, '%') THEN 3
      WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
       AND REGEXP_CONTAINS(REPLACE(SRC_TABLE_NAME, '__', '_'), CONCAT('_', REGEXP_EXTRACT(columns_target.column_name, r'_(\w+)$'))) IS TRUE THEN 4
      WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') THEN 5
    ELSE 99 
  END ASC) AS SORT_RANK_ID,

FROM entities_union

LEFT JOIN --- Source Column List (Utilizing Info Schema Directy Because Full Column Population Required)
  `PROJECT_ID.region-us`.INFORMATION_SCHEMA.COLUMNS columns_src
  ON entities_union.SRC_SCHEMA_NAME = columns_src.table_schema
  AND entities_union.SRC_TABLE_NAME = columns_src.table_name
  AND columns_src.column_name NOT LIKE '%ROW_%'

LEFT JOIN --- Target Column List
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS columns_target
  ON entities_union.TRGT_SCHEMA_NAME = columns_target.table_schema
  AND entities_union.TRGT_TABLE_NAME = columns_target.table_name
  AND columns_target.column_name NOT LIKE '%ROW_%'

WHERE 1=1
AND columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') -- Required to remove the cartisan join used to create the matches
),

airflow AS
(
SELECT

sub_task_target_dataset_id,
sub_task_target_table_id,
sproc_name,
dag_name,
task_name,
sub_task_start_time,
  ROW_NUMBER() OVER (
    PARTITION BY sub_task_target_dataset_id,
                 sub_task_target_table_id
    ORDER BY sub_task_start_time DESC
  )AS SORT_RANK_ID,


FROM `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T
WHERE 1=1
AND sub_task_target_table_id <> 'LOGTBL_DIMENSION'
AND sub_task_target_dataset_id NOT LIKE '%_script%'
order by 4 desc
),

FINAL as(

SELECT 
    SRC_SCHEMA_NAME,
    SRC_TABLE_NAME,
    SRC_COLUMN_NAME,
    SRC_COLUMN_ORDINAL_POSITION,
    trgt_dataset.schema_name_group AS TRGT_SCHEMA_NAME_PARENT,
    TRGT_SCHEMA_NAME,
    TRGT_TABLE_NAME,
    TRGT_COLUMN_NAME,
    TRGT_COLUMN_ORDINAL_POSITION,
    TRGT_COLUMN_TECH_DESC,
    looker.vw_description AS TRGT_COLUMN_BUSINESS_DESC,
    UPPER(looker.vw_schema) AS VW_SCHEMA_NAME,
    UPPER(looker.vw_entity_name) AS VW_TABLE_NAME,
    UPPER(looker.vw_column_name) AS VW_COLUMN_NAME,
    logic.MATCH_RANK_ID,
    logic.SORT_RANK_ID,
    trgt_dataset.pdw_flag AS TRGT_PDW_FLAG,
    airflow.sproc_name AS STORED_PROCEDURE_NAME,
    airflow.dag_name AS DAG_NAME,
    airflow.task_name AS DAG_TASK_NAME,
    DATE(airflow.sub_task_start_time) AS DAG_LAST_RUN_DTE,
    trgt_entity.create_date AS TRGT_TABLE_CREATE_DTE,
    trgt_entity.last_modify_date AS TRGT_TABLE_LAST_MODIFY_DTE,
    trgt_entity.total_rows AS TRGT_TABLE_ROWS,
    'https://github.com/CardinalHealth/pharma-data-warehouse-edna/blob/master/'||trgt_dataset.schema_name_group AS TRGT_GITHUB_REPO,


FROM logic
LEFT JOIN
 `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V looker -- D1 Through VW to Looker Mapping
    ON TRGT_SCHEMA_NAME = UPPER(looker.D1_schema)
    AND TRGT_TABLE_NAME = UPPER(looker.D1_entity_name)
    AND TRGT_COLUMN_NAME = UPPER(looker.D1_column_name)
    AND TRGT_TABLE_NAME = UPPER(looker.VW_entity_name)

LEFT JOIN
 `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T trgt_entity -- Table Master Data
    ON TRGT_SCHEMA_NAME = trgt_entity.entity_schema
    AND TRGT_TABLE_NAME = trgt_entity.entity_name
    AND trgt_entity.entity_type LIKE '%TABLE%'

LEFT JOIN
 `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` trgt_dataset -- Schema Master Data and PDW Filtering
    ON TRGT_SCHEMA_NAME = trgt_dataset.SCHEMA_NAME

LEFT JOIN airflow
  ON sub_task_target_dataset_id = TRGT_SCHEMA_NAME
  AND sub_task_target_table_id = TRGT_TABLE_NAME
  AND TRGT_TABLE_NAME <> 'LOGTBL_DIMENSION'
  AND airflow.SORT_RANK_ID = 1
 
WHERE 1=1
 AND logic.SORT_RANK_ID = 1 -- Remove duplicates by ranking by highest quality match logic
 AND trgt_dataset.pdw_flag IS TRUE -- Only pdw entities
)
 SELECT * FROM FINAL 
 WHERE FINAL.TRGT_TABLE_NAME = 'CONTRACT_CV' 
 --AND TRGT_COLUMN_NAME LIKE '%VTEXT_TVAST%'
 ;


