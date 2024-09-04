CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_TASK_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_TASK_T;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_TASK_T
SELECT
  snapshot_date,
  snapshot_timestamp,
  creation_time,
  project_id,
  project_number,
  user_email,
  job_id,
  job_type,
  statement_type,
  start_time,
  end_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS excution_time_seconds,
  TIMESTAMP_DIFF(end_time, start_time, MINUTE) AS excution_time_minutes,
  query,
  state,
  reservation_id,
  total_bytes_processed,
  total_bytes_processed / POW(1024, 3) AS total_gigabytes_processed,
  total_bytes_billed / POW(1024, 3) AS total_gigabytes_billed,
  total_slot_ms,
  total_slot_ms / (1000 * 60 * 60 * 24) AS slots,
  total_bytes_billed,
  parent_job_id,
  total_modified_partitions,
  destination_table,
  dml_statistics,
  error_result,
  CASE WHEN query LIKE '%D1_PHM_DW_LOG%' THEN TRUE ELSE FALSE END AS logging_insert_flag,
  CASE WHEN error_result.reason IS NULL THEN 'PASS' ELSE 'FAIL' END AS job_success_flag,

FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T`

WHERE 1=1
  AND TIMESTAMP_TRUNC(start_time, DAY) > TIMESTAMP("2023-09-01")
  AND (user_email LIKE '%sa-edna-pharma-data-whse%' OR user_email LIKE '%@edc-p-oe-%')
  AND statement_type <> 'SELECT'
  AND parent_job_id IS NOT NULL
;
END;