CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_TABLES_STORAGE_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T

SELECT

  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS snapshot_timestamp,
  FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME,CURRENT_TIMESTAMP())) AS pk,
  FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME)) AS pk_table,
  ds.schema_name_group,
  ds.dataset_group,
  ds.dataset_group_ad_np,
  ds.pdw_flag,
  t.*,

FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.TABLE_STORAGE t
  LEFT JOIN
     `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
    ON
      t.table_schema = ds.SCHEMA_NAME
    AND
      ds.pdw_flag IS TRUE

;
END;