
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_LOGTBL_DIMENSION_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for LOGTBL_DIMENSION
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.LOGTBL_DIMENSION_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` AS target
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
        TARGET.MODULE_NAME = SOURCE.MODULE_NAME,
        TARGET.START_DATE = SOURCE.START_DATE,
        TARGET.END_DATE = SOURCE.END_DATE,
        TARGET.STATUS = SOURCE.STATUS,
        TARGET.MERGE_ROWCOUNT = SOURCE.MERGE_ROWCOUNT,
        TARGET.INSERT_ROWCOUNT = SOURCE.INSERT_ROWCOUNT,
        TARGET.UPDATE_ROWCOUNT = SOURCE.UPDATE_ROWCOUNT,
        TARGET.DELETE_ROWCOUNT = SOURCE.DELETE_ROWCOUNT,
        TARGET.MB_PROCESSED = SOURCE.MB_PROCESSED,
        TARGET.MB_BILLED = SOURCE.MB_BILLED,
        TARGET.JOB_ID = SOURCE.JOB_ID,
        TARGET.CUSTOM1 = SOURCE.CUSTOM1,
        TARGET.CUSTOM2 = SOURCE.CUSTOM2,
        TARGET.ERR_MSG = SOURCE.ERR_MSG,
        TARGET.ERR_ST_TXT = SOURCE.ERR_ST_TXT,
        TARGET.ERR_ST_TRC = SOURCE.ERR_ST_TRC
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.MODULE_NAME,
        SOURCE.START_DATE,
        SOURCE.END_DATE,
        SOURCE.STATUS,
        SOURCE.MERGE_ROWCOUNT,
        SOURCE.INSERT_ROWCOUNT,
        SOURCE.UPDATE_ROWCOUNT,
        SOURCE.DELETE_ROWCOUNT,
        SOURCE.MB_PROCESSED,
        SOURCE.MB_BILLED,
        SOURCE.JOB_ID,
        SOURCE.CUSTOM1,
        SOURCE.CUSTOM2,
        SOURCE.ERR_MSG,
        SOURCE.ERR_ST_TXT,
        SOURCE.ERR_ST_TRC
      );
    END;
