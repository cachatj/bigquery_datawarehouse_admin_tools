CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_VIEWS_MERGE()
BEGIN
MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_VIEWS_T AS target
    USING(
        SELECT
            CURRENT_DATE() AS snapshot_date,
            CURRENT_TIMESTAMP() AS snapshot_timestamp,
            FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME,CURRENT_TIMESTAMP())) AS pk,
            FARM_FINGERPRINT(CONCAT(table_catalog, table_schema,TABLE_NAME)) AS pk_table,
            v.table_catalog,
            v.table_schema,
            v.table_name,
            v.view_definition,
            v.check_option,
            v.use_standard_sql,
        FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.VIEWS v
            JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
               ON v.table_schema = ds.SCHEMA_NAME
        WHERE ds.pdw_flag IS TRUE
    )
    AS SOURCE
    ON  target.table_catalog = source.table_catalog
    AND target.table_schema = source.table_schema
    AND target.table_name = source.table_name
WHEN NOT MATCHED THEN
INSERT
    (
        snapshot_date,
        snapshot_timestamp,
        pk,
        pk_table,
        table_catalog,
        table_schema,
        TABLE_NAME,
        view_definition,
        check_option,
        use_standard_sql
    )
VALUES
    (
        source.snapshot_date,
        source.snapshot_timestamp,
        source.pk,
        source.pk_table,
        source.table_catalog,
        source.table_schema,
        source.table_name,
        source.view_definition,
        source.check_option,
        source.use_standard_sql
    )
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE;

END;