CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_TABLE_STORAGE_HISTORY_INSERT()
BEGIN
  -- Insert new records when the source row count for each pk_table changes
  INSERT INTO `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_HISTORY_T` (
    snapshot_date,
    snapshot_timestamp,
    pk_table,
    table_catalog,
    table_schema,
    TABLE_NAME,
    total_rows,
    total_partitions,
    total_logical_bytes,
    active_logical_bytes,
    long_term_logical_bytes,
    total_physical_bytes,
    active_physical_bytes,
    long_term_physical_bytes,
    time_travel_physical_bytes,
    storage_last_modified_time
  )
  SELECT
    CURRENT_DATE() AS snapshot_date,
    CURRENT_TIMESTAMP() AS snapshot_timestamp,
    source.pk_table,
    source.table_catalog,
    source.table_schema,
    source.table_name,
    source.total_rows,
    source.total_partitions,
    source.total_logical_bytes,
    source.active_logical_bytes,
    source.long_term_logical_bytes,
    source.total_physical_bytes,
    source.active_physical_bytes,
    source.long_term_physical_bytes,
    source.time_travel_physical_bytes,
    source.storage_last_modified_time
  FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T` AS SOURCE
  LIMIT 0;

END;