
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_TABLE_STORAGE_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_TABLE_STORAGE_T
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLE_STORAGE_T` AS target
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
        TARGET.schema_name_group = SOURCE.schema_name_group,
        TARGET.dataset_group = SOURCE.dataset_group,
        TARGET.dataset_group_ad_np = SOURCE.dataset_group_ad_np,
        TARGET.pdw_flag = SOURCE.pdw_flag,
        TARGET.project_id = SOURCE.project_id,
        TARGET.project_number = SOURCE.project_number,
        TARGET.table_catalog = SOURCE.table_catalog,
        TARGET.table_schema = SOURCE.table_schema,
        TARGET.table_name = SOURCE.table_name,
        TARGET.creation_time = SOURCE.creation_time,
        TARGET.total_rows = SOURCE.total_rows,
        TARGET.total_partitions = SOURCE.total_partitions,
        TARGET.total_logical_bytes = SOURCE.total_logical_bytes,
        TARGET.active_logical_bytes = SOURCE.active_logical_bytes,
        TARGET.long_term_logical_bytes = SOURCE.long_term_logical_bytes,
        TARGET.total_physical_bytes = SOURCE.total_physical_bytes,
        TARGET.active_physical_bytes = SOURCE.active_physical_bytes,
        TARGET.long_term_physical_bytes = SOURCE.long_term_physical_bytes,
        TARGET.time_travel_physical_bytes = SOURCE.time_travel_physical_bytes,
        TARGET.storage_last_modified_time = SOURCE.storage_last_modified_time,
        TARGET.deleted = SOURCE.deleted,
        TARGET.table_type = SOURCE.table_type,
        TARGET.fail_safe_physical_bytes = SOURCE.fail_safe_physical_bytes
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.pk,
        SOURCE.pk_table,
        SOURCE.schema_name_group,
        SOURCE.dataset_group,
        SOURCE.dataset_group_ad_np,
        SOURCE.pdw_flag,
        SOURCE.project_id,
        SOURCE.project_number,
        SOURCE.table_catalog,
        SOURCE.table_schema,
        SOURCE.table_name,
        SOURCE.creation_time,
        SOURCE.total_rows,
        SOURCE.total_partitions,
        SOURCE.total_logical_bytes,
        SOURCE.active_logical_bytes,
        SOURCE.long_term_logical_bytes,
        SOURCE.total_physical_bytes,
        SOURCE.active_physical_bytes,
        SOURCE.long_term_physical_bytes,
        SOURCE.time_travel_physical_bytes,
        SOURCE.storage_last_modified_time,
        SOURCE.deleted,
        SOURCE.table_type,
        SOURCE.fail_safe_physical_bytes
      );
    END;
