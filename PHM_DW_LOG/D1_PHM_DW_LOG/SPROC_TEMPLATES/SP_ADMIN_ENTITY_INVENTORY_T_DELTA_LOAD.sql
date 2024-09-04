
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_ENTITY_INVENTORY_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_ENTITY_INVENTORY_T
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_INVENTORY_T` AS target
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
        TARGET.entity_catalog = SOURCE.entity_catalog,
        TARGET.schema_name_group = SOURCE.schema_name_group,
        TARGET.dataset_group = SOURCE.dataset_group,
        TARGET.dataset_group_ad_np = SOURCE.dataset_group_ad_np,
        TARGET.entity_schema = SOURCE.entity_schema,
        TARGET.entity_name = SOURCE.entity_name,
        TARGET.entity_type = SOURCE.entity_type,
        TARGET.entity_ddl = SOURCE.entity_ddl,
        TARGET.create_date = SOURCE.create_date,
        TARGET.last_modify_date = SOURCE.last_modify_date,
        TARGET.days_since_last_modify = SOURCE.days_since_last_modify,
        TARGET.days_since_create_date = SOURCE.days_since_create_date,
        TARGET.total_rows = SOURCE.total_rows,
        TARGET.drop_entity_sql = SOURCE.drop_entity_sql,
        TARGET.pdw_flag = SOURCE.pdw_flag,
        TARGET.dag_name = SOURCE.dag_name,
        TARGET.task_name = SOURCE.task_name,
        TARGET.dag_last_run_date = SOURCE.dag_last_run_date,
        TARGET.dag_last_run_success_date = SOURCE.dag_last_run_success_date,
        TARGET.dag_status = SOURCE.dag_status
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.entity_catalog,
        SOURCE.schema_name_group,
        SOURCE.dataset_group,
        SOURCE.dataset_group_ad_np,
        SOURCE.entity_schema,
        SOURCE.entity_name,
        SOURCE.entity_type,
        SOURCE.entity_ddl,
        SOURCE.create_date,
        SOURCE.last_modify_date,
        SOURCE.days_since_last_modify,
        SOURCE.days_since_create_date,
        SOURCE.total_rows,
        SOURCE.drop_entity_sql,
        SOURCE.pdw_flag,
        SOURCE.dag_name,
        SOURCE.task_name,
        SOURCE.dag_last_run_date,
        SOURCE.dag_last_run_success_date,
        SOURCE.dag_status
      );
    END;
