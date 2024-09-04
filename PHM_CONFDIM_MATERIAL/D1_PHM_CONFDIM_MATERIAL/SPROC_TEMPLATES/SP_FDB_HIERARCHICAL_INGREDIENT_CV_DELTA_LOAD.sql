
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SP_FDB_HIERARCHICAL_INGREDIENT_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for FDB_HIERARCHICAL_INGREDIENT_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV` AS target
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
        TARGET.NDC = SOURCE.NDC,
        TARGET.HIC_SEQN = SOURCE.HIC_SEQN,
        TARGET.HIC_REL_NO = SOURCE.HIC_REL_NO,
        TARGET.HIC = SOURCE.HIC,
        TARGET.HIC_DESC = SOURCE.HIC_DESC,
        TARGET.HIC_ROOT = SOURCE.HIC_ROOT,
        TARGET.HIC_POTENTIALLY_INACTV_IND = SOURCE.HIC_POTENTIALLY_INACTV_IND,
        TARGET.ING_STATUS_CD = SOURCE.ING_STATUS_CD,
        TARGET.CAS9_TBL = SOURCE.CAS9_TBL,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.NDC,
        SOURCE.HIC_SEQN,
        SOURCE.HIC_REL_NO,
        SOURCE.HIC,
        SOURCE.HIC_DESC,
        SOURCE.HIC_ROOT,
        SOURCE.HIC_POTENTIALLY_INACTV_IND,
        SOURCE.ING_STATUS_CD,
        SOURCE.CAS9_TBL,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
