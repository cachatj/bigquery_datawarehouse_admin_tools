
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for PRIMARY_BACKORDER_SELECTION_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV` AS target
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
        TARGET.YYBOPRE1_YTSD_BOPRE1_TEXT = SOURCE.YYBOPRE1_YTSD_BOPRE1_TEXT,
        TARGET.YYBOPRE1_DES_YTSD_BOPRE1_TEXT = SOURCE.YYBOPRE1_DES_YTSD_BOPRE1_TEXT,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.YYBOPRE1_YTSD_BOPRE1_TEXT,
        SOURCE.YYBOPRE1_DES_YTSD_BOPRE1_TEXT,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
