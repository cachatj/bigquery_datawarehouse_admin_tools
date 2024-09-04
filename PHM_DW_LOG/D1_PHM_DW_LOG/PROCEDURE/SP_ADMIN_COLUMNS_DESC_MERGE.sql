CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_COLUMNS_DESC_MERGE()
BEGIN
  MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_DESC_T AS target
  USING(
    SELECT
      CURRENT_DATE() AS snapshot_date,
      CURRENT_TIMESTAMP() AS snapshot_timestamp,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, COLUMN_NAME, CURRENT_TIMESTAMP())) AS pk,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME)) AS pk_table,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, COLUMN_NAME)) AS pk_table_column,
      fp.table_catalog,
      fp.table_schema,
      fp.table_name,
      fp.column_name,
      fp.field_path,
      fp.data_type,
      fp.description,
      fp.collation_name,
      fp.rounding_mode,

    FROM  `PROJECT_ID.region-us`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS fp
      JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
       ON fp.table_schema = ds.SCHEMA_NAME
    WHERE ds.pdw_flag IS TRUE
  ) AS SOURCE
  ON target.table_catalog = source.table_catalog
  AND target.table_schema = source.table_schema
  AND target.table_name = source.table_name
  AND target.column_name = source.column_name
  AND target.description = source.description
  WHEN NOT MATCHED THEN
    INSERT
      (
        snapshot_date,
        snapshot_timestamp,
        pk,
        pk_table,
        pk_table_column,
        table_catalog,
        table_schema,
        TABLE_NAME,
        COLUMN_NAME,
        field_path,
        data_type,
        description,
        COLLATION_NAME,
        rounding_mode

      )
    VALUES
      (
        source.snapshot_date,
        source.snapshot_timestamp,
        source.pk,
        source.pk_table,
        source.pk_table_column,
        source.table_catalog,
        source.table_schema,
        source.table_name,
        source.column_name,
        source.field_path,
        source.data_type,
        source.description,
        source.collation_name,
        source.rounding_mode
      )
 WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
END;