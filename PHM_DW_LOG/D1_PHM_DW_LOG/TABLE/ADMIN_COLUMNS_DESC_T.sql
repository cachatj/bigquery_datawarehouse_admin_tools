CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_DESC_T`
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
  field_path STRING,
  data_type STRING,
  description STRING,
  COLLATION_NAME STRING,
  rounding_mode STRING
)
PARTITION BY snapshot_date
CLUSTER BY table_schema, TABLE_NAME, COLUMN_NAME, field_path;