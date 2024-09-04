
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_AIRFLOW_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_JOBS_PDW_AIRFLOW_T
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_JOBS_PDW_AIRFLOW_T` AS target
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
        TARGET.dag_run_date = SOURCE.dag_run_date,
        TARGET.dag_last_run_success_date = SOURCE.dag_last_run_success_date,
        TARGET.dag_run_time = SOURCE.dag_run_time,
        TARGET.sproc_project = SOURCE.sproc_project,
        TARGET.sproc_dataset = SOURCE.sproc_dataset,
        TARGET.sproc_name = SOURCE.sproc_name,
        TARGET.dag_name = SOURCE.dag_name,
        TARGET.task_name = SOURCE.task_name,
        TARGET.dag_success_flag = SOURCE.dag_success_flag,
        TARGET.sub_task_statement_type = SOURCE.sub_task_statement_type,
        TARGET.sub_task_success_flag = SOURCE.sub_task_success_flag,
        TARGET.sub_task_start_time = SOURCE.sub_task_start_time,
        TARGET.sub_task_end_time = SOURCE.sub_task_end_time,
        TARGET.sub_task_excution_time_seconds = SOURCE.sub_task_excution_time_seconds,
        TARGET.sub_task_query = SOURCE.sub_task_query,
        TARGET.sub_task_target_project_id = SOURCE.sub_task_target_project_id,
        TARGET.sub_task_target_dataset_id = SOURCE.sub_task_target_dataset_id,
        TARGET.sub_task_target_table_id = SOURCE.sub_task_target_table_id,
        TARGET.sub_task_inserted_row_count = SOURCE.sub_task_inserted_row_count,
        TARGET.sub_task_deleted_row_count = SOURCE.sub_task_deleted_row_count,
        TARGET.sub_task_updated_row_count = SOURCE.sub_task_updated_row_count,
        TARGET.sub_task_error_reason = SOURCE.sub_task_error_reason,
        TARGET.sub_task_error_message = SOURCE.sub_task_error_message,
        TARGET.task_logging_insert_flag = SOURCE.task_logging_insert_flag,
        TARGET.dag_total_slot_ms = SOURCE.dag_total_slot_ms,
        TARGET.sub_task_total_slot_ms = SOURCE.sub_task_total_slot_ms
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.dag_run_date,
        SOURCE.dag_last_run_success_date,
        SOURCE.dag_run_time,
        SOURCE.sproc_project,
        SOURCE.sproc_dataset,
        SOURCE.sproc_name,
        SOURCE.dag_name,
        SOURCE.task_name,
        SOURCE.dag_success_flag,
        SOURCE.sub_task_statement_type,
        SOURCE.sub_task_success_flag,
        SOURCE.sub_task_start_time,
        SOURCE.sub_task_end_time,
        SOURCE.sub_task_excution_time_seconds,
        SOURCE.sub_task_query,
        SOURCE.sub_task_target_project_id,
        SOURCE.sub_task_target_dataset_id,
        SOURCE.sub_task_target_table_id,
        SOURCE.sub_task_inserted_row_count,
        SOURCE.sub_task_deleted_row_count,
        SOURCE.sub_task_updated_row_count,
        SOURCE.sub_task_error_reason,
        SOURCE.sub_task_error_message,
        SOURCE.task_logging_insert_flag,
        SOURCE.dag_total_slot_ms,
        SOURCE.sub_task_total_slot_ms
      );
    END;
