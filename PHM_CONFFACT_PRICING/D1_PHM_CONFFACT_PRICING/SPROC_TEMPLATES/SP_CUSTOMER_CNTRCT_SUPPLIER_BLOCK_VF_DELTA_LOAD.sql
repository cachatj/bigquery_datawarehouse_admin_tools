
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF
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
    SET v_stored_proc_name = 'D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF` AS target
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
        TARGET.KNUMA_AG_A719 = SOURCE.KNUMA_AG_A719,
        TARGET.VKORG_A719 = SOURCE.VKORG_A719,
        TARGET.VTEXT_TVKOT = SOURCE.VTEXT_TVKOT,
        TARGET.VTWEG_A719 = SOURCE.VTWEG_A719,
        TARGET.VTEXT_TVTWT = SOURCE.VTEXT_TVTWT,
        TARGET.SPART_A719 = SOURCE.SPART_A719,
        TARGET.VTEXT_TSPAT = SOURCE.VTEXT_TSPAT,
        TARGET.KUNAG_A719 = SOURCE.KUNAG_A719,
        TARGET.IRM_PCNUM_A719 = SOURCE.IRM_PCNUM_A719,
        TARGET.LIFNR_A719 = SOURCE.LIFNR_A719,
        TARGET.DATAB_A719 = SOURCE.DATAB_A719,
        TARGET.DATBI_A719 = SOURCE.DATBI_A719,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID,
        TARGET.LOEVM_KO_KONP = SOURCE.LOEVM_KO_KONP
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.KNUMA_AG_A719,
        SOURCE.VKORG_A719,
        SOURCE.VTEXT_TVKOT,
        SOURCE.VTWEG_A719,
        SOURCE.VTEXT_TVTWT,
        SOURCE.SPART_A719,
        SOURCE.VTEXT_TSPAT,
        SOURCE.KUNAG_A719,
        SOURCE.IRM_PCNUM_A719,
        SOURCE.LIFNR_A719,
        SOURCE.DATAB_A719,
        SOURCE.DATBI_A719,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID,
        SOURCE.LOEVM_KO_KONP
      );
    END;
