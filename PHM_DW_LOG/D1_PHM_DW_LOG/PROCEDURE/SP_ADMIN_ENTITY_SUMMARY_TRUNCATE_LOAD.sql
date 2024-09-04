CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_ENTITY_SUMMARY_TRUNCATE_LOAD()
BEGIN
TRUNCATE TABLE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ENTITY_SUMMARY_T;
INSERT INTO `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ENTITY_SUMMARY_T

WITH

attributes_count AS(

SELECT

FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME)) AS pk_table,
COUNT(DISTINCT COLUMN_NAME) AS total_columns

FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.COLUMNS
GROUP BY 1

)

-- final join of table and attribute metadata

SELECT
  t.snapshot_date, -- date query was executed
  t.snapshot_timestamp, -- date and time query was executed
  t.pk, -- primary key of tbl including the date of the query
  t.pk_table, -- primary key of the table
  t.table_catalog,
  t.table_schema,
  ds.schema_name_group,
  ds.dataset_group,
  ds.dataset_group_ad_np,
  t.table_name,
  t.table_type,
  t.is_insertable_into,
  t.is_typed,
  t.creation_time,
  t.snapshot_time_ms,
  t.ddl,
  v.view_definition,
  t.upsert_stream_apply_watermark,
  ac.total_columns,
  s.total_rows,
  s.total_partitions,
  s.total_logical_bytes,
  s.active_logical_bytes,
  s.long_term_logical_bytes,
  s.total_physical_bytes,
  s.active_physical_bytes,
  s.long_term_physical_bytes,
  s.time_travel_physical_bytes,
  s.total_logical_bytes / (1024 * 1024) AS total_logical_mb,
  s.active_logical_bytes / (1024 * 1024) AS active_logical_mb,
  s.long_term_logical_bytes / (1024 * 1024) AS long_term_logical_mb,
  s.total_physical_bytes / (1024 * 1024) AS total_physical_mb,
  s.active_physical_bytes / (1024 * 1024) AS active_physical_mb,
  s.long_term_physical_bytes / (1024 * 1024) AS long_term_physical_mb,
  s.time_travel_physical_bytes / (1024 * 1024) AS time_travel_physical_mb,
  s.total_logical_bytes / (1024 * 1024) AS total_logical_gb,
  s.active_logical_bytes / (1024 * 1024) AS active_logical_gb,
  s.long_term_logical_bytes / (1024 * 1024) AS long_term_logical_gb,
  s.total_physical_bytes / (1024 * 1024) AS total_physical_gb,
  s.active_physical_bytes / (1024 * 1024) AS active_physical_gb,
  s.long_term_physical_bytes / (1024 * 1024) AS long_term_physical_gb,
  s.time_travel_physical_bytes / (1024 * 1024) AS time_travel_physical_gb,
  s.active_logical_bytes / (1024 * 1024 * 1024) * 0.02 AS active_logical_cost,
  s.long_term_logical_bytes / (1024 * 1024 * 1024) * 0.01 AS long_term_logical_cost,
  s.active_physical_bytes / (1024 * 1024 * 1024) * 0.04 AS active_physical_cost,
  s.long_term_physical_bytes / (1024 * 1024 * 1024) * 0.02 AS long_term_physical_cost,
  s.storage_last_modified_time,

FROM  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T` t
 LEFT JOIN
   `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T`  s
    ON
      t.pk_table = s.pk_table

  LEFT JOIN
    `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_VIEWS_T`   v
    ON
      t.pk_table = v.pk_table

  LEFT JOIN
    attributes_count ac
    ON
      t.pk_table = ac.pk_table

  LEFT JOIN
     `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
    ON
      t.table_schema = ds.SCHEMA_NAME
    AND
      ds.pdw_flag IS TRUE;

END;