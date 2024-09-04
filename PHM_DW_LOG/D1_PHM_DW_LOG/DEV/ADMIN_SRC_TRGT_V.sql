CREATE OR REPLACE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLLIBRA_T`
AS
WITH entities AS (
    SELECT
        dataset_id AS ReferencedDatasetID,
        table_id AS ReferencedTableID,
        admin_jobs_t.destination_table.dataset_id AS DestinationDatasetID,
        admin_jobs_t.destination_table.table_id AS DestinationTableID,
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
  ReferencedDatasetID,
  ReferencedTableID,
  columns_src.column_name AS ReferencedColumnID,
  columns_target.column_name AS TargetColumnID,
  DestinationDatasetID,
  DestinationTableID,

   CASE
    WHEN ReferencedTableID = DestinationTableID THEN 98
    WHEN columns_src.column_name = columns_target.column_name THEN 1
    WHEN CONCAT(REPLACE(columns_src.column_name,"_CV",""),'_',ReferencedTableID) LIKE  CONCAT('%',columns_target.column_name, '%') THEN 2
    
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND  columns_target.column_name LIKE CONCAT('%',ReferencedTableID, '%') THEN 3

    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND REGEXP_CONTAINS(REPLACE(ReferencedTableID, '__', '_'), CONCAT('_', REGEXP_EXTRACT(columns_target.column_name, r'_(\w+)$'))) IS TRUE THEN 4
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') THEN 5
    ELSE 99 
  END AS match_rank,

  ROW_NUMBER() OVER (
    PARTITION BY DestinationDatasetID,
                 DestinationTableID,
                 columns_target.column_name
   ORDER BY 
   CASE
    WHEN ReferencedTableID = DestinationTableID THEN 98
    WHEN columns_src.column_name = columns_target.column_name THEN 1
    WHEN CONCAT(REPLACE(columns_src.column_name,"_CV",""),'_',ReferencedTableID) LIKE  CONCAT('%',columns_target.column_name, '%') THEN 2
    
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND  columns_target.column_name LIKE CONCAT('%',ReferencedTableID, '%') THEN 3

    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
     AND REGEXP_CONTAINS(REPLACE(ReferencedTableID, '__', '_'), CONCAT('_', REGEXP_EXTRACT(columns_target.column_name, r'_(\w+)$'))) IS TRUE THEN 4
    WHEN columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') THEN 5
    ELSE 99 
  END ASC
    ) AS row_num,

FROM entities

LEFT JOIN --- Source Column List
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_src
  ON entities.ReferencedDatasetID = columns_src.table_schema
  AND entities.ReferencedTableID = columns_src.table_name
  AND columns_src.column_name NOT LIKE '%ROW_%'

LEFT JOIN --- Target Column List
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_target
  ON entities.DestinationDatasetID = columns_target.table_schema
  AND entities.DestinationTableID = columns_target.table_name
  AND columns_target.column_name NOT LIKE '%ROW_%'

WHERE 1=1
AND columns_target.column_name LIKE CONCAT(columns_src.column_name, '%') 
)

SELECT * FROM logic where row_num = 1



