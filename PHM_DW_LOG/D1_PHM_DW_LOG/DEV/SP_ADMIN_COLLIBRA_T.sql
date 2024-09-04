CREATE OR REPLACE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLLIBRA_T`
AS
WITH entities AS (
    SELECT
        dataset_id AS SRC_SCHEMA_NAME,
        table_id AS SRC_TABLE_NAME,
        admin_jobs_t.destination_table.dataset_id AS TRGT_SCHEMA_NAME,
        admin_jobs_t.destination_table.table_id AS TRGT_TABLE_NAME,
        COUNT(DISTINCT admin_jobs_t.job_id) AS JobCount
    FROM
     `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T` AS admin_jobs_t
        LEFT JOIN UNNEST(admin_jobs_t.referenced_tables) AS ReferencedTables
    WHERE 1=1
        AND table_id IS NOT NULL
        --AND admin_jobs_t.destination_table.table_id = 'CREDIT_CONTROL_AREA_CV'
        AND dataset_id NOT LIKE '%_script%'
        AND admin_jobs_t.destination_table.dataset_id NOT LIKE '%VW_%'
    GROUP BY
        1, 2, 3, 4
    ORDER BY
        3 DESC
),

logic
AS(

SELECT DISTINCT
  SRC_SCHEMA_NAME,
  SRC_TABLE_NAME,
  columns_src.column_name AS SRC_COLUMN_NAME,
  columns_target.column_name AS TRGT_COLUMN_NAME,
  TRGT_SCHEMA_NAME,
  TRGT_TABLE_NAME,
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
  ROW_NUMBER() OVER (
    PARTITION BY TRGT_SCHEMA_NAME,
                 TRGT_TABLE_NAME,
                 columns_target.column_name
    ORDER BY 
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
  END ASC) AS ROW_NUM,

FROM entities

LEFT JOIN --- Source Column List
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_src
  ON entities.SRC_SCHEMA_NAME = columns_src.table_schema
  AND entities.SRC_TABLE_NAME = columns_src.table_name
  AND columns_src.column_name NOT LIKE '%ROW_%'

LEFT JOIN --- Target Column List
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_target
  ON entities.TRGT_SCHEMA_NAME = columns_target.table_schema
  AND entities.TRGT_TABLE_NAME = columns_target.table_name
  AND columns_target.column_name NOT LIKE '%ROW_%'

WHERE 1=1
AND columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
)

SELECT 

    SRC_SCHEMA_NAME,
    SRC_TABLE_NAME,
    SRC_COLUMN_NAME,
    trgt_dataset.schema_name_group AS TRGT_SCHEMA_NAME_PARENT,
    TRGT_SCHEMA_NAME,
    TRGT_TABLE_NAME,
    TRGT_COLUMN_NAME,
    looker.d1_description AS TRGT_COLUMN_TECH_DESC,
    looker.vw_description AS TRGT_COLUMN_BUSINESS_DESC,
    UPPER(looker.vw_schema) AS VW_SCHEMA_NAME,
    UPPER(looker.vw_entity_name) AS VW_TABLE_NAME,
    UPPER(looker.vw_column_name) AS VW_COLUMN_NAME,
    MATCH_RANK_ID,
    ROW_NUM,
    trgt_dataset.pdw_flag AS TRGT_PDW_FLAG,

FROM logic
LEFT JOIN
 `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V` looker -- D1 Through VW to Looker Mapping
    ON TRGT_SCHEMA_NAME = UPPER(looker.D1_schema)
    AND TRGT_TABLE_NAME = UPPER(looker.D1_entity_name)
    AND TRGT_COLUMN_NAME = UPPER(looker.D1_column_name)
LEFT JOIN
 `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` trgt_dataset
    ON TRGT_SCHEMA_NAME = trgt_dataset.SCHEMA_NAME
 

WHERE 1=1 
 AND ROW_NUM = 1
 AND trgt_dataset.pdw_flag IS TRUE



