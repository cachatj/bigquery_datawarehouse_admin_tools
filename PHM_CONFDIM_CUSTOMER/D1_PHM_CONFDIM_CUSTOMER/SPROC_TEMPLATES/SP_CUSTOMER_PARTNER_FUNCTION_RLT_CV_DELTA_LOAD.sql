
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_PARTNER_FUNCTION_RLT_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CUSTOMER_PARTNER_FUNCTION_RLT_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT_CV` AS target
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
        TARGET.KUNNR_KNVP = SOURCE.KUNNR_KNVP,
        TARGET.VKORG_KNVP = SOURCE.VKORG_KNVP,
        TARGET.VTWEG_KNVP = SOURCE.VTWEG_KNVP,
        TARGET.SPART_KNVP = SOURCE.SPART_KNVP,
        TARGET.PARVW_KNVP = SOURCE.PARVW_KNVP,
        TARGET.VRSN_START_DTE = SOURCE.VRSN_START_DTE,
        TARGET.VRSN_END_DTE = SOURCE.VRSN_END_DTE,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.VTEXT_TPART_PARTNER_FUNCTION_CODE = SOURCE.VTEXT_TPART_PARTNER_FUNCTION_CODE,
        TARGET.KUNN2_KNVP = SOURCE.KUNN2_KNVP,
        TARGET.PARZA_KNVP = SOURCE.PARZA_KNVP,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.KUNNR_KNVP,
        SOURCE.VKORG_KNVP,
        SOURCE.VTWEG_KNVP,
        SOURCE.SPART_KNVP,
        SOURCE.PARVW_KNVP,
        SOURCE.VRSN_START_DTE,
        SOURCE.VRSN_END_DTE,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.VTEXT_TPART_PARTNER_FUNCTION_CODE,
        SOURCE.KUNN2_KNVP,
        SOURCE.PARZA_KNVP,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
