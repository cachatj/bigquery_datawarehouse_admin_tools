CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_ROUTINES_MERGE()
BEGIN
  MERGE `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_ROUTINES_T AS target
  USING(
    SELECT
      CURRENT_DATE() AS snapshot_date,
      CURRENT_TIMESTAMP() AS snapshot_timestamp,
      FARM_FINGERPRINT(CONCAT(ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, CURRENT_TIMESTAMP())) AS pk,
      FARM_FINGERPRINT(CONCAT(ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME)) AS pk_table,
      r.specific_catalog,
      r.specific_schema,
      r.specific_name,
      r.routine_catalog,
      r.routine_schema,
      r.routine_name,
      r.routine_type,
      r.data_type,
      r.routine_body,
      r.routine_definition,
      r.external_language,
      r.is_deterministic,
      r.security_type,
      r.created,
      r.last_altered,
      r.ddl,
      r.connection
    FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.ROUTINES r
      JOIN `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V` ds
       ON r.routine_schema = ds.SCHEMA_NAME
    WHERE ds.pdw_flag IS TRUE
  ) AS SOURCE
  ON target.routine_catalog = source.routine_catalog
  AND target.routine_schema = source.routine_schema
  AND target.routine_name = source.routine_name
  WHEN MATCHED AND (target.created != source.created OR target.last_altered != source.last_altered) THEN
    UPDATE SET
      snapshot_date = source.snapshot_date,
      snapshot_timestamp = source.snapshot_timestamp,
      pk = source.pk,
      pk_table = source.pk_table,
      specific_catalog = source.specific_catalog,
      specific_schema = source.specific_schema,
      SPECIFIC_NAME = source.specific_name,
      ROUTINE_CATALOG = source.routine_catalog,
      ROUTINE_SCHEMA = source.routine_schema,
      ROUTINE_NAME = source.routine_name,
      routine_type = source.routine_type,
      data_type = source.data_type,
      routine_body = source.routine_body,
      routine_definition = source.routine_definition,
      external_language = source.external_language,
      is_deterministic = source.is_deterministic,
      security_type = source.security_type,
      created = source.created,
      last_altered = source.last_altered,
      ddl = source.ddl,
      CONNECTION = source.connection
  WHEN NOT MATCHED THEN
    INSERT
      (
        snapshot_date,
        snapshot_timestamp,
        pk,
        pk_table,
        specific_catalog,
        specific_schema,
        SPECIFIC_NAME,
        ROUTINE_CATALOG,
        ROUTINE_SCHEMA,
        ROUTINE_NAME,
        routine_type,
        data_type,
        routine_body,
        routine_definition,
        external_language,
        is_deterministic,
        security_type,
        created,
        last_altered,
        ddl,
        CONNECTION
      )
    VALUES
      (
        source.snapshot_date,
        source.snapshot_timestamp,
        source.pk,
        source.pk_table,
        source.specific_catalog,
        source.specific_schema,
        source.specific_name,
        source.routine_catalog,
        source.routine_schema,
        source.routine_name,
        source.routine_type,
        source.data_type,
        source.routine_body,
        source.routine_definition,
        source.external_language,
        source.is_deterministic,
        source.security_type,
        source.created,
        source.last_altered,
        source.ddl,
        source.connection
      )
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
END;