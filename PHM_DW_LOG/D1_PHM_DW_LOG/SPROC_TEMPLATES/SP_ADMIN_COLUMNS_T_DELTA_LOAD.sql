
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_COLUMNS_T_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_COLUMNS_T
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_COLUMNS_T_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_T`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_COLUMNS_T` AS target
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
        TARGET.pk_table_column = SOURCE.pk_table_column,
        TARGET.table_catalog = SOURCE.table_catalog,
        TARGET.table_schema = SOURCE.table_schema,
        TARGET.table_name = SOURCE.table_name,
        TARGET.column_name = SOURCE.column_name,
        TARGET.ordinal_position = SOURCE.ordinal_position,
        TARGET.is_nullable = SOURCE.is_nullable,
        TARGET.data_type = SOURCE.data_type,
        TARGET.is_generated = SOURCE.is_generated,
        TARGET.generation_expression = SOURCE.generation_expression,
        TARGET.is_stored = SOURCE.is_stored,
        TARGET.is_hidden = SOURCE.is_hidden,
        TARGET.is_updatable = SOURCE.is_updatable,
        TARGET.is_system_defined = SOURCE.is_system_defined,
        TARGET.is_partitioning_column = SOURCE.is_partitioning_column,
        TARGET.clustering_ordinal_position = SOURCE.clustering_ordinal_position,
        TARGET.collation_name = SOURCE.collation_name,
        TARGET.column_default = SOURCE.column_default,
        TARGET.rounding_mode = SOURCE.rounding_mode
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.snapshot_date,
        SOURCE.snapshot_timestamp,
        SOURCE.pk,
        SOURCE.pk_table,
        SOURCE.pk_table_column,
        SOURCE.table_catalog,
        SOURCE.table_schema,
        SOURCE.table_name,
        SOURCE.column_name,
        SOURCE.ordinal_position,
        SOURCE.is_nullable,
        SOURCE.data_type,
        SOURCE.is_generated,
        SOURCE.generation_expression,
        SOURCE.is_stored,
        SOURCE.is_hidden,
        SOURCE.is_updatable,
        SOURCE.is_system_defined,
        SOURCE.is_partitioning_column,
        SOURCE.clustering_ordinal_position,
        SOURCE.collation_name,
        SOURCE.column_default,
        SOURCE.rounding_mode
      );
    END;
