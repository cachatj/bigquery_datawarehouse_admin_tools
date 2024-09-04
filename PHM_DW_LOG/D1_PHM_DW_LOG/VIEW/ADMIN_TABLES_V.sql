CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_V`
AS SELECT

CURRENT_DATE() AS snapshot_date,
CURRENT_TIMESTAMP() AS snapshot_timestamp,
FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME,CURRENT_TIMESTAMP())) AS pk,
FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME)) AS pk_table,
*,

FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.TABLES;