CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_VIEWS_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  table_catalog STRING,
  table_schema STRING,
  TABLE_NAME STRING,
  view_definition STRING,
  check_option STRING,
  use_standard_sql STRING
)
PARTITION BY snapshot_date
CLUSTER BY table_schema, TABLE_NAME;