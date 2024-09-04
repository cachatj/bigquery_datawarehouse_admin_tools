
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_TABLES_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_TABLES_T
    Data Sources:  See SQL Below

    ************************************************************************************************************************/
    DECLARE v_start_time timestamp;
    DECLARE v_start_stp timestamp;
    DECLARE v_end_stp timestamp;
    DECLARE v_stored_proc_name STRING;
    DECLARE last_load_timestamp timestamp;

    SET v_start_time = current_timestamp();
    SET v_start_stp = current_timestamp();
    SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_TABLES_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_T` AS target
     USING 
      (
     -- Developer's code goes here (SAMPLE CODE BELOW)

         --SELECT 
            a.column_1
            a.column_2
         --FROM 
         -- `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_SDW__INVOICE_LINE_CV` il
         --JOIN
            --`PROJECT_ID.VI2_PHM_CONFDIM_TIME.PHM_SDW__TIME_DETAIL_CV` t
                --ON il.DTE_KEY_NUM = t.DTE_KEY_NUM
        --WHERE il.D0_UPDATE_STP > last_load_timestamp --- MOST IMPORTANT CONCEPT

      ) AS source
      ON TARGET.PRIMARY_KEY = SOURCE.PRIMARY_KEY
    WHEN MATCHED THEN
      UPDATE SET
        TARGET.snapshot_date = SOURCE.snapshot_date,
        TARGET.snapshot_timestamp = SOURCE.snapshot_timestamp,
        TARGET.pk = SOURCE.pk,
        TARGET.pk_table = SOURCE.pk_table,
        TARGET.table_catalog = SOURCE.table_catalog,
        TARGET.table_schema = SOURCE.table_schema,
        TARGET.table_name = SOURCE.table_name,
        TARGET.table_type = SOURCE.table_type,
        TARGET.is_insertable_into = SOURCE.is_insertable_into,
        TARGET.is_typed = SOURCE.is_typed,
        TARGET.creation_time = SOURCE.creation_time,
        TARGET.base_table_catalog = SOURCE.base_table_catalog,
        TARGET.base_table_schema = SOURCE.base_table_schema,
        TARGET.base_table_name = SOURCE.base_table_name,
        TARGET.snapshot_time_ms = SOURCE.snapshot_time_ms,
        TARGET.ddl = SOURCE.ddl,
        TARGET.default_collation_name = SOURCE.default_collation_name,
        TARGET.upsert_stream_apply_watermark = SOURCE.upsert_stream_apply_watermark
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.pk,
        SOURCE.pk_table,
        SOURCE.table_catalog,
        SOURCE.table_schema,
        SOURCE.table_name,
        SOURCE.table_type,
        SOURCE.is_insertable_into,
        SOURCE.is_typed,
        SOURCE.creation_time,
        SOURCE.base_table_catalog,
        SOURCE.base_table_schema,
        SOURCE.base_table_name,
        SOURCE.snapshot_time_ms,
        SOURCE.ddl,
        SOURCE.default_collation_name,
        SOURCE.upsert_stream_apply_watermark
      );
    END;
