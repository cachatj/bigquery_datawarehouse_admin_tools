CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  table_catalog STRING,
  table_schema STRING,
  TABLE_NAME STRING,
  table_type STRING,
  is_insertable_into STRING,
  is_typed STRING,
  creation_time TIMESTAMP,
  base_table_catalog STRING,
  base_table_schema STRING,
  base_table_name STRING,
  snapshot_time_ms TIMESTAMP,
  ddl STRING,
  default_collation_name STRING,
  upsert_stream_apply_watermark TIMESTAMP
)
PARTITION BY DATE(creation_time)
CLUSTER BY table_schema, TABLE_NAME, table_type;