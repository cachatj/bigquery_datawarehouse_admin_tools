CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_ENTITY_INVENTORY_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T

--- Retrieve Dag Metadata From Airflow Jobs View (Union Table and Sproc Details)
WITH
dag_list
AS(
SELECT
  'stored_proc' AS entity_type,
  sproc_dataset AS data_set,
  sproc_name AS entity_name,
  STRING_AGG(DISTINCT dag_name) AS dag_name,
  STRING_AGG(DISTINCT task_name) AS task_name,
  MAX(dag_run_date) AS dag_last_run_date,
  MAX(dag_last_run_success_date) AS dag_last_run_success_date,

FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T`
GROUP BY 1,2,3

UNION ALL

SELECT
  'table' AS entity_type,
  sub_task_target_dataset_id AS data_set,
  sub_task_target_table_id AS entity_name,
  STRING_AGG(DISTINCT dag_name) AS dag_name,
  STRING_AGG(DISTINCT task_name) AS task_name,
  MAX(DATE(sub_task_start_time)) AS dag_last_run_date,
  MAX(dag_last_run_success_date) AS dag_last_run_success_date,
FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T`
 JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T`
  ON sub_task_target_dataset_id = table_schema
  AND sub_task_target_table_id = TABLE_NAME
GROUP BY 1,2,3
),
--- Retrieve Entity Master Data (Union Tables/Views with Procedures)
entity
AS
(
SELECT
    table_catalog,
    table_schema,
    TABLE_NAME,
    CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS table_type,
    ddl,
    DATE(creation_time) AS create_date,
    CASE
    WHEN table_type = 'BASE TABLE' AND storage_last_modified_time IS NULL THEN DATE(creation_time)
    WHEN table_type = 'VIEW' AND storage_last_modified_time IS NULL THEN DATE(creation_time)
    ELSE DATE(storage_last_modified_time)
    END AS last_modify_date,
    total_rows,
FROM
 `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_SUMMARY_T`
UNION ALL
SELECT
    ROUTINE_CATALOG,
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    routine_type,
    ddl,
    DATE(created) AS create_date,
    DATE(last_altered) AS last_modify_date,
    0 AS total_rows
FROM
`PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ROUTINES_T`
)
--- Join Entity Metadata, Dataset Master Data and Airflow Logging Results
SELECT
    CURRENT_DATE() AS snapshot_date,
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    table_catalog AS entity_catalog,
    schema_name_group,
    ds.dataset_group,
    ds.dataset_group_ad_np,
    table_schema AS entity_schema,
    TABLE_NAME AS entity_name,
    table_type AS entity_type,
    entity.ddl AS entity_ddl,
    create_date,
    last_modify_date,
    DATE_DIFF(CURRENT_DATE(),last_modify_date, DAY) AS days_since_last_modify,
    DATE_DIFF(CURRENT_DATE(),create_date, DAY) AS days_since_create_date,
    total_rows,
    'DROP '||table_type||' '||table_schema||'.'||TABLE_NAME||';' AS drop_entity_sql,
    ds.pdw_flag,
    dag.dag_name,
    dag.task_name,
    dag.dag_last_run_date,
    dag.dag_last_run_success_date,
    CASE
     WHEN table_type NOT IN('TABLE', 'PROCEDURE') THEN 'DAG_NOT_REQUIRED'
     WHEN dag.dag_last_run_date IS NULL AND table_type IN('TABLE','PROCEDURE') THEN 'DAG_MISSING'
     WHEN dag.dag_last_run_date < create_date THEN 'DAG_NEEDS_RERUN'
     WHEN dag.dag_last_run_date IS NOT NULL AND dag.dag_last_run_success_date IS NULL THEN 'DAG_BROKEN'
     WHEN dag.dag_last_run_date > dag.dag_last_run_success_date THEN 'DAG_BROKEN'
    ELSE 'DAG_SUCCESS' END AS dag_status,
FROM entity
LEFT JOIN dag_list dag
    ON table_schema = dag.data_set
    AND TABLE_NAME = dag.entity_name
JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
  ON table_schema = SCHEMA_NAME
  AND pdw_flag IS TRUE;

END;