CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_AIRFLOW_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T

SELECT DISTINCT
  DATE(dags.creation_time) AS dag_run_date,
  CASE WHEN dags.dag_success_flag = 'PASS' THEN DATE(dags.creation_time) ELSE NULL END AS dag_last_run_success_date,
  dags.creation_time AS dag_run_time,
  dags.sproc_project,
  dags.sproc_dataset,
  dags.sproc_name,
  dags.dag_name,
  dags.task_name,
  --dags.task_run_time,
  dags.dag_success_flag,
  tasks.statement_type AS sub_task_statement_type,
  tasks.job_success_flag AS sub_task_success_flag,
  tasks.start_time AS sub_task_start_time,
  tasks.end_time AS sub_task_end_time,
  tasks.excution_time_seconds AS sub_task_excution_time_seconds,
  tasks.query AS sub_task_query,
  tasks.destination_table.project_id AS sub_task_target_project_id,
  tasks.destination_table.dataset_id AS sub_task_target_dataset_id,
  tasks.destination_table.table_id AS sub_task_target_table_id,
  tasks.dml_statistics.inserted_row_count AS sub_task_inserted_row_count,
  tasks.dml_statistics.deleted_row_count AS sub_task_deleted_row_count,
  tasks.dml_statistics.updated_row_count AS sub_task_updated_row_count,
  tasks.error_result.reason AS sub_task_error_reason,
  tasks.error_result.message AS sub_task_error_message,
  tasks.logging_insert_flag AS task_logging_insert_flag,
  dags.total_slot_ms AS dag_total_slot_ms,
  tasks.total_slot_ms AS sub_task_total_slot_ms,

FROM
  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_DAG_T` dags
LEFT JOIN
  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_TASK_T` tasks
ON
  dags.job_id = tasks.parent_job_id
WHERE
  1=1
  AND DATE(dags.creation_time) > '2023-09-01'
  AND tasks.query IS NOT NULL;

END;