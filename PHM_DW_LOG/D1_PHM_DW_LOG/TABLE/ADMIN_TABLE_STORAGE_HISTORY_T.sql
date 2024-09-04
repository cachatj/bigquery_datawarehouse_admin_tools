CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_HISTORY_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk_table INT64,
  table_catalog STRING,
  table_schema STRING,
  TABLE_NAME STRING,
  total_rows INT64,
  total_partitions INT64,
  total_logical_bytes INT64,
  active_logical_bytes INT64,
  long_term_logical_bytes INT64,
  total_physical_bytes INT64,
  active_physical_bytes INT64,
  long_term_physical_bytes INT64,
  time_travel_physical_bytes INT64,
  storage_last_modified_time TIMESTAMP
)
PARTITION BY snapshot_date
CLUSTER BY table_schema, TABLE_NAME;