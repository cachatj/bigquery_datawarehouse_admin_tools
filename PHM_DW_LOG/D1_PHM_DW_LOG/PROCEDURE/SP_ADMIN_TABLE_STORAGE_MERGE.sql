CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_TABLE_STORAGE_MERGE()
BEGIN
  MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T AS target
  USING (
    SELECT
      CURRENT_DATE() AS snapshot_date,
      CURRENT_TIMESTAMP() AS snapshot_timestamp,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, CURRENT_TIMESTAMP())) AS pk,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME)) AS pk_table,
      ds.schema_name_group,
      ds.dataset_group,
      ds.dataset_group_ad_np,
      ds.pdw_flag,
      t.project_id,
      t.project_number,
      t.table_catalog,
      t.table_schema,
      t.table_name,
      t.creation_time,
      t.total_rows,
      t.total_partitions,
      t.total_logical_bytes,
      t.active_logical_bytes,
      t.long_term_logical_bytes,
      t.total_physical_bytes,
      t.active_physical_bytes,
      t.long_term_physical_bytes,
      t.time_travel_physical_bytes,
      t.storage_last_modified_time,
      t.deleted,
      t.table_type,
      t.fail_safe_physical_bytes
    FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.TABLE_STORAGE t
     JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
      ON t.table_schema = ds.SCHEMA_NAME
    WHERE ds.pdw_flag IS TRUE
  ) AS SOURCE
  ON target.table_catalog = source.table_catalog
  AND target.table_schema = source.table_schema
  AND target.TABLE_NAME = source.TABLE_NAME

  WHEN MATCHED AND target.storage_last_modified_time < source.storage_last_modified_time THEN
    UPDATE SET
      target.snapshot_date = source.snapshot_date,
      target.snapshot_timestamp = source.snapshot_timestamp,
      target.pk_table = source.pk_table,
      target.schema_name_group = source.schema_name_group,
      target.dataset_group = source.dataset_group,
      target.dataset_group_ad_np = source.dataset_group_ad_np,
      target.pdw_flag = source.pdw_flag,
      target.project_id = source.project_id,
      target.project_number = source.project_number,
      target.table_catalog = source.table_catalog,
      target.table_schema = source.table_schema,
      target.table_name = source.table_name,
      target.creation_time = source.creation_time,
      target.total_rows = source.total_rows,
      target.total_partitions = source.total_partitions,
      target.total_logical_bytes = source.total_logical_bytes,
      target.active_logical_bytes = source.active_logical_bytes,
      target.long_term_logical_bytes = source.long_term_logical_bytes,
      target.total_physical_bytes = source.total_physical_bytes,
      target.active_physical_bytes = source.active_physical_bytes,
      target.long_term_physical_bytes = source.long_term_physical_bytes,
      target.time_travel_physical_bytes = source.time_travel_physical_bytes,
      target.storage_last_modified_time = source.storage_last_modified_time,
      target.deleted = source.deleted,
      target.table_type = source.table_type,
      target.fail_safe_physical_bytes = source.fail_safe_physical_bytes
  WHEN NOT MATCHED THEN
    INSERT (
      snapshot_date,
      snapshot_timestamp,
      pk,
      pk_table,
      schema_name_group,
      dataset_group,
      dataset_group_ad_np,
      pdw_flag,
      project_id,
      project_number,
      table_catalog,
      table_schema,
      TABLE_NAME,
      creation_time,
      total_rows,
      total_partitions,
      total_logical_bytes,
      active_logical_bytes,
      long_term_logical_bytes,
      total_physical_bytes,
      active_physical_bytes,
      long_term_physical_bytes,
      time_travel_physical_bytes,
      storage_last_modified_time,
      deleted,
      table_type,
      fail_safe_physical_bytes
    ) VALUES (
      source.snapshot_date,
      source.snapshot_timestamp,
      source.pk,
      source.pk_table,
      source.schema_name_group,
      source.dataset_group,
      source.dataset_group_ad_np,
      source.pdw_flag,
      source.project_id,
      source.project_number,
      source.table_catalog,
      source.table_schema,
      source.table_name,
      source.creation_time,
      source.total_rows,
      source.total_partitions,
      source.total_logical_bytes,
      source.active_logical_bytes,
      source.long_term_logical_bytes,
      source.total_physical_bytes,
      source.active_physical_bytes,
      source.long_term_physical_bytes,
      source.time_travel_physical_bytes,
      source.storage_last_modified_time,
      source.deleted,
      source.table_type,
      source.fail_safe_physical_bytes
    )
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
END;