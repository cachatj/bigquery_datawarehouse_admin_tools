CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  pk_table_column INT64,
  table_catalog STRING,
  table_schema STRING,
  TABLE_NAME STRING,
  COLUMN_NAME STRING,
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
PARTITION BY snapshot_date
CLUSTER BY table_schema, TABLE_NAME, COLUMN_NAME;