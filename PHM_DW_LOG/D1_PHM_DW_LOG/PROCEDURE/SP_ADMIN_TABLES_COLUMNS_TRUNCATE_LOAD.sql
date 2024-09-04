CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_TABLES_COLUMNS_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS

WITH attribute_desc AS (
      SELECT DISTINCT
        snapshot_date,
        snapshot_timestamp,
        pk,
        pk_table,
        pk_table_column,
        description
      FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_DESC_T`
    )
SELECT
      t.snapshot_date,
      t.snapshot_timestamp,
      c.pk,
      t.pk_table,
      c.pk_table_column,
      t.table_catalog,
      ds.schema_name_group,
      ds.dataset_group,
      ds.dataset_group_ad_np,
      ds.pdw_flag,
      t.table_schema,
      t.table_name,
      t.table_type,
      t.is_insertable_into,
      t.is_typed,
      t.creation_time,
      t.snapshot_time_ms,
      t.ddl,
      v.view_definition,
      c.column_name,
      d.description,
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
    FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T` t
    LEFT JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_VIEWS_T` v
      ON t.pk_table = v.pk_table
    LEFT JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_T` c
      ON t.pk_table = c.pk_table
    LEFT JOIN attribute_desc d
      ON c.pk_table_column = d.pk_table_column
    JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
      ON t.table_schema = ds.SCHEMA_NAME
      AND ds.pdw_flag IS TRUE;
END;