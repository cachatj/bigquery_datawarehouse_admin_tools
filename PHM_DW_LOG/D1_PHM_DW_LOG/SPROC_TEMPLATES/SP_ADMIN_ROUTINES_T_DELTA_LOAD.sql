
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_ROUTINES_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_ROUTINES_T
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_ROUTINES_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ROUTINES_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ROUTINES_T` AS target
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
        TARGET.specific_catalog = SOURCE.specific_catalog,
        TARGET.specific_schema = SOURCE.specific_schema,
        TARGET.specific_name = SOURCE.specific_name,
        TARGET.routine_catalog = SOURCE.routine_catalog,
        TARGET.routine_schema = SOURCE.routine_schema,
        TARGET.routine_name = SOURCE.routine_name,
        TARGET.routine_type = SOURCE.routine_type,
        TARGET.data_type = SOURCE.data_type,
        TARGET.routine_body = SOURCE.routine_body,
        TARGET.routine_definition = SOURCE.routine_definition,
        TARGET.external_language = SOURCE.external_language,
        TARGET.is_deterministic = SOURCE.is_deterministic,
        TARGET.security_type = SOURCE.security_type,
        TARGET.created = SOURCE.created,
        TARGET.last_altered = SOURCE.last_altered,
        TARGET.ddl = SOURCE.ddl,
        TARGET.connection = SOURCE.connection
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.pk,
        SOURCE.pk_table,
        SOURCE.specific_catalog,
        SOURCE.specific_schema,
        SOURCE.specific_name,
        SOURCE.routine_catalog,
        SOURCE.routine_schema,
        SOURCE.routine_name,
        SOURCE.routine_type,
        SOURCE.data_type,
        SOURCE.routine_body,
        SOURCE.routine_definition,
        SOURCE.external_language,
        SOURCE.is_deterministic,
        SOURCE.security_type,
        SOURCE.created,
        SOURCE.last_altered,
        SOURCE.ddl,
        SOURCE.connection
      );
    END;
