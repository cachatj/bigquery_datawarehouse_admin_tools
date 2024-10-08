CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  pk_table_column INT64,
  table_catalog STRING,
  schema_name_group STRING,
  dataset_group STRING,
  dataset_group_ad_np STRING,
  pdw_flag BOOL,
  table_schema STRING,
  TABLE_NAME STRING,
  table_type STRING,
  is_insertable_into STRING,
  is_typed STRING,
  creation_time TIMESTAMP,
  snapshot_time_ms TIMESTAMP,
  ddl STRING,
  view_definition STRING,
  COLUMN_NAME STRING,
  description STRING,
  ordinal_position INT64,
  is_nullable STRING,
  data_type STRING,
  is_generated STRING,
  generation_expression STRING,
  is_stored STRING,
  is_hidden STRING,
  is_updatable STRING,
  is_system_defined STRING,
  is_partitioning_column STRING,
  clustering_ordinal_position INT64,
  COLLATION_NAME STRING,
  column_default STRING,
  rounding_mode STRING
)
PARTITION BY DATE(creation_time)
CLUSTER BY pdw_flag, schema_name_group, dataset_group, TABLE_NAME;