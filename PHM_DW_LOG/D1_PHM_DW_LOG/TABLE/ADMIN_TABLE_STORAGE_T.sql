CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  schema_name_group STRING,
  dataset_group STRING,
  dataset_group_ad_np STRING,
  pdw_flag BOOL,
  project_id STRING,
  project_number INT64,
  table_catalog STRING,
  table_schema STRING,
  TABLE_NAME STRING,
  creation_time TIMESTAMP,
  total_rows INT64,
  total_partitions INT64,
  total_logical_bytes INT64,
  active_logical_bytes INT64,
  long_term_logical_bytes INT64,
  total_physical_bytes INT64,
  active_physical_bytes INT64,
  long_term_physical_bytes INT64,
  time_travel_physical_bytes INT64,
  storage_last_modified_time TIMESTAMP,
  deleted BOOL,
  table_type STRING,
  fail_safe_physical_bytes INT64
)
PARTITION BY DATE(creation_time)
CLUSTER BY pdw_flag, schema_name_group, dataset_group, TABLE_NAME;