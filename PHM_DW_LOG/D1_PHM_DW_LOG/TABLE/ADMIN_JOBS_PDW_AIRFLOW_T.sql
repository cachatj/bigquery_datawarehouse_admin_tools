CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T`
(
  dag_run_date DATE,
  dag_last_run_success_date DATE,
  dag_run_time TIMESTAMP,
  sproc_project STRING,
  sproc_dataset STRING,
  sproc_name STRING,
  dag_name STRING,
  task_name STRING,
  dag_success_flag STRING,
  sub_task_statement_type STRING,
  sub_task_success_flag STRING,
  sub_task_start_time TIMESTAMP,
  sub_task_end_time TIMESTAMP,
  sub_task_excution_time_seconds INT64,
  sub_task_query STRING,
  sub_task_target_project_id STRING,
  sub_task_target_dataset_id STRING,
  sub_task_target_table_id STRING,
  sub_task_inserted_row_count INT64,
  sub_task_deleted_row_count INT64,
  sub_task_updated_row_count INT64,
  sub_task_error_reason STRING,
  sub_task_error_message STRING,
  task_logging_insert_flag BOOL,
  dag_total_slot_ms INT64,
  sub_task_total_slot_ms INT64
)
PARTITION BY dag_run_date
CLUSTER BY dag_name, task_name;