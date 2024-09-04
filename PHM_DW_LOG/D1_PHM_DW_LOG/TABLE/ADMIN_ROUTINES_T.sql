CREATE TABLE `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ROUTINES_T`
(
  snapshot_date DATE,
  snapshot_timestamp TIMESTAMP,
  pk INT64,
  pk_table INT64,
  specific_catalog STRING,
  specific_schema STRING,
  SPECIFIC_NAME STRING,
  ROUTINE_CATALOG STRING,
  ROUTINE_SCHEMA STRING,
  ROUTINE_NAME STRING,
  routine_type STRING,
  data_type STRING,
  routine_body STRING,
  routine_definition STRING,
  external_language STRING,
  is_deterministic STRING,
  security_type STRING,
  created TIMESTAMP,
  last_altered TIMESTAMP,
  ddl STRING,
  CONNECTION STRING
)
PARTITION BY DATE(created)
CLUSTER BY ROUTINE_SCHEMA, ROUTINE_NAME, routine_type;