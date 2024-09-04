
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_BUSINESS_ACTIVITY_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for BUSINESS_ACTIVITY
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.BUSINESS_ACTIVITY_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.BUSINESS_ACTIVITY`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.BUSINESS_ACTIVITY` AS target
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
        TARGET.BUSINESS_ACTIVITY_CDE = SOURCE.BUSINESS_ACTIVITY_CDE,
        TARGET.BUSINESS_ACTIVITY_SUB_CDE = SOURCE.BUSINESS_ACTIVITY_SUB_CDE,
        TARGET.BUSINESS_ACTIVITY_DESC_TXT = SOURCE.BUSINESS_ACTIVITY_DESC_TXT
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.BUSINESS_ACTIVITY_CDE,
        SOURCE.BUSINESS_ACTIVITY_SUB_CDE,
        SOURCE.BUSINESS_ACTIVITY_DESC_TXT
      );
    END;
