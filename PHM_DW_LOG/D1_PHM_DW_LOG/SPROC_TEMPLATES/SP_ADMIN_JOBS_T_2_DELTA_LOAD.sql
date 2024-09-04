
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_JOBS_T_2_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_JOBS_T_2
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_JOBS_T_2_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T_2`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_T_2` AS target
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
        TARGET.creation_time = SOURCE.creation_time,
        TARGET.project_id = SOURCE.project_id,
        TARGET.project_number = SOURCE.project_number,
        TARGET.user_email = SOURCE.user_email,
        TARGET.job_id = SOURCE.job_id,
        TARGET.job_type = SOURCE.job_type,
        TARGET.statement_type = SOURCE.statement_type,
        TARGET.priority = SOURCE.priority,
        TARGET.start_time = SOURCE.start_time,
        TARGET.end_time = SOURCE.end_time,
        TARGET.query = SOURCE.query,
        TARGET.state = SOURCE.state,
        TARGET.reservation_id = SOURCE.reservation_id,
        TARGET.total_bytes_processed = SOURCE.total_bytes_processed,
        TARGET.total_slot_ms = SOURCE.total_slot_ms,
        TARGET.error_result = SOURCE.error_result,
        TARGET.cache_hit = SOURCE.cache_hit,
        TARGET.destination_table = SOURCE.destination_table,
        TARGET.referenced_tables = SOURCE.referenced_tables,
        TARGET.labels = SOURCE.labels,
        TARGET.timeline = SOURCE.timeline,
        TARGET.job_stages = SOURCE.job_stages,
        TARGET.total_bytes_billed = SOURCE.total_bytes_billed,
        TARGET.transaction_id = SOURCE.transaction_id,
        TARGET.parent_job_id = SOURCE.parent_job_id,
        TARGET.session_info = SOURCE.session_info,
        TARGET.dml_statistics = SOURCE.dml_statistics,
        TARGET.total_modified_partitions = SOURCE.total_modified_partitions,
        TARGET.bi_engine_statistics = SOURCE.bi_engine_statistics,
        TARGET.query_info = SOURCE.query_info,
        TARGET.transferred_bytes = SOURCE.transferred_bytes,
        TARGET.materialized_view_statistics = SOURCE.materialized_view_statistics,
        TARGET.edition = SOURCE.edition,
        TARGET.job_creation_reason = SOURCE.job_creation_reason
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.creation_time,
        SOURCE.project_id,
        SOURCE.project_number,
        SOURCE.user_email,
        SOURCE.job_id,
        SOURCE.job_type,
        SOURCE.statement_type,
        SOURCE.priority,
        SOURCE.start_time,
        SOURCE.end_time,
        SOURCE.query,
        SOURCE.state,
        SOURCE.reservation_id,
        SOURCE.total_bytes_processed,
        SOURCE.total_slot_ms,
        SOURCE.error_result,
        SOURCE.cache_hit,
        SOURCE.destination_table,
        SOURCE.referenced_tables,
        SOURCE.labels,
        SOURCE.timeline,
        SOURCE.job_stages,
        SOURCE.total_bytes_billed,
        SOURCE.transaction_id,
        SOURCE.parent_job_id,
        SOURCE.session_info,
        SOURCE.dml_statistics,
        SOURCE.total_modified_partitions,
        SOURCE.bi_engine_statistics,
        SOURCE.query_info,
        SOURCE.transferred_bytes,
        SOURCE.materialized_view_statistics,
        SOURCE.edition,
        SOURCE.job_creation_reason
      );
    END;
