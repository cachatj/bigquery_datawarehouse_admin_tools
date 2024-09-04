
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_RETAIL_TIER_PRICE_VF_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for MATERIAL_RETAIL_TIER_PRICE_VF
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.MATERIAL_RETAIL_TIER_PRICE_VF_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_RETAIL_TIER_PRICE_VF`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_RETAIL_TIER_PRICE_VF` AS target
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
        TARGET.MATNR_A714 = SOURCE.MATNR_A714,
        TARGET.VKORG_A714 = SOURCE.VKORG_A714,
        TARGET.VTWEG_A714 = SOURCE.VTWEG_A714,
        TARGET.SPART_A714 = SOURCE.SPART_A714,
        TARGET.YYRPRPT_A714 = SOURCE.YYRPRPT_A714,
        TARGET.YYRPRPT_DESC_YTSD_HAMACHER_ZL = SOURCE.YYRPRPT_DESC_YTSD_HAMACHER_ZL,
        TARGET.DATAB_A714 = SOURCE.DATAB_A714,
        TARGET.DATBI_A714 = SOURCE.DATBI_A714,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.KBETR_KONP = SOURCE.KBETR_KONP,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.MATNR_A714,
        SOURCE.VKORG_A714,
        SOURCE.VTWEG_A714,
        SOURCE.SPART_A714,
        SOURCE.YYRPRPT_A714,
        SOURCE.YYRPRPT_DESC_YTSD_HAMACHER_ZL,
        SOURCE.DATAB_A714,
        SOURCE.DATBI_A714,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.KBETR_KONP,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
