CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_TABLES_MERGE()
BEGIN
  MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLES_T AS target
  USING (
    SELECT
      CURRENT_DATE() AS snapshot_date,
      CURRENT_TIMESTAMP() AS snapshot_timestamp,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, CURRENT_TIMESTAMP())) AS pk,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME)) AS pk_table,
      t.table_catalog,
      t.table_schema,
      t.table_name,
      t.table_type,
      t.is_insertable_into,
      t.is_typed,
      t.creation_time,
      t.base_table_catalog,
      t.base_table_schema,
      t.base_table_name,
      t.snapshot_time_ms,
      t.ddl,
      t.default_collation_name,
      t.upsert_stream_apply_watermark
    FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.TABLES t
    JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
       ON t.table_schema = ds.SCHEMA_NAME
    WHERE ds.pdw_flag IS TRUE
  ) AS SOURCE
  ON target.table_catalog = source.table_catalog
  AND target.table_schema = source.table_schema
  AND target.table_name = source.table_name

  WHEN MATCHED AND target.creation_time < source.creation_time THEN
    UPDATE SET
      target.snapshot_date = source.snapshot_date,
      target.snapshot_timestamp = source.snapshot_timestamp,
      target.pk_table = source.pk_table,
      target.table_catalog = source.table_catalog,
      target.table_schema = source.table_schema,
      target.table_name = source.table_name,
      target.table_type = source.table_type,
      target.is_insertable_into = source.is_insertable_into,
      target.is_typed = source.is_typed,
      target.creation_time = source.creation_time,
      target.base_table_catalog = source.base_table_catalog,
      target.base_table_schema = source.base_table_schema,
      target.base_table_name = source.base_table_name,
      target.snapshot_time_ms = source.snapshot_time_ms,
      target.ddl = source.ddl,
      target.default_collation_name = source.default_collation_name,
      target.upsert_stream_apply_watermark = source.upsert_stream_apply_watermark
  WHEN NOT MATCHED THEN
    INSERT (
      snapshot_date,
      snapshot_timestamp,
      pk,
      pk_table,
      table_catalog,
      table_schema,
      TABLE_NAME,
      table_type,
      is_insertable_into,
      is_typed,
      creation_time,
      base_table_catalog,
      base_table_schema,
      base_table_name,
      snapshot_time_ms,
      ddl,
      default_collation_name,
      upsert_stream_apply_watermark
    ) VALUES (
      source.snapshot_date,
      source.snapshot_timestamp,
      source.pk,
      source.pk_table,
      source.table_catalog,
      source.table_schema,
      source.table_name,
      source.table_type,
      source.is_insertable_into,
      source.is_typed,
      source.creation_time,
      source.base_table_catalog,
      source.base_table_schema,
      source.base_table_name,
      source.snapshot_time_ms,
      source.ddl,
      source.default_collation_name,
      source.upsert_stream_apply_watermark
    )
    WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
END;