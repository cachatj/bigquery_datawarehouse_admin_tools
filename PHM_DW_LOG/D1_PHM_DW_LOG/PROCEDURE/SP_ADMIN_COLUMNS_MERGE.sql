CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_COLUMNS_MERGE()
BEGIN
  MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T AS target
  USING(
    SELECT
      CURRENT_DATE() AS snapshot_date,
      CURRENT_TIMESTAMP() AS snapshot_timestamp,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, COLUMN_NAME, CURRENT_TIMESTAMP())) AS pk,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME)) AS pk_table,
      FARM_FINGERPRINT(CONCAT(table_catalog, table_schema, TABLE_NAME, COLUMN_NAME)) AS pk_table_column,
      c.table_catalog,
      c.table_schema,
      c.table_name,
      c.column_name,
      c.ordinal_position,
      c.is_nullable,
      c.data_type,
      c.is_generated,
      c.generation_expression,
      c.is_stored,
      c.is_hidden,
      c.is_updatable,
      c.is_system_defined,
      c.is_partitioning_column,
      c.clustering_ordinal_position,
      c.collation_name,
      c.column_default,
      c.rounding_mode
    FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.COLUMNS c
      JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
       ON c.table_schema = ds.SCHEMA_NAME
    WHERE ds.pdw_flag IS TRUE
  ) AS SOURCE
  ON target.table_catalog = source.table_catalog
  AND target.table_schema = source.table_schema
  AND target.table_name = source.table_name
  AND target.column_name = source.column_name
  WHEN MATCHED THEN
    UPDATE SET
      target.snapshot_date = source.snapshot_date,
      target.snapshot_timestamp = source.snapshot_timestamp,
      target.pk = source.pk,
      target.pk_table = source.pk_table,
      target.pk_table_column = source.pk_table_column,
      target.table_catalog = source.table_catalog,
      target.table_schema = source.table_schema,
      target.table_name = source.table_name,
      target.column_name = source.column_name,
      target.ordinal_position = source.ordinal_position,
      target.is_nullable = source.is_nullable,
      target.data_type = source.data_type,
      target.is_generated = source.is_generated,
      target.generation_expression = source.generation_expression,
      target.is_stored = source.is_stored,
      target.is_hidden = source.is_hidden,
      target.is_updatable = source.is_updatable,
      target.is_system_defined = source.is_system_defined,
      target.is_partitioning_column = source.is_partitioning_column,
      target.clustering_ordinal_position = source.clustering_ordinal_position,
      target.collation_name = source.collation_name,
      target.column_default = source.column_default,
      target.rounding_mode = source.rounding_mode
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
        ordinal_position,
        is_nullable,
        data_type,
        is_generated,
        generation_expression,
        is_stored,
        is_hidden,
        is_updatable,
        is_system_defined,
        is_partitioning_column,
        clustering_ordinal_position,
        COLLATION_NAME,
        column_default,
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
        source.ordinal_position,
        source.is_nullable,
        source.data_type,
        source.is_generated,
        source.generation_expression,
        source.is_stored,
        source.is_hidden,
        source.is_updatable,
        source.is_system_defined,
        source.is_partitioning_column,
        source.clustering_ordinal_position,
        source.collation_name,
        source.column_default,
        source.rounding_mode
      )
 WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
END;