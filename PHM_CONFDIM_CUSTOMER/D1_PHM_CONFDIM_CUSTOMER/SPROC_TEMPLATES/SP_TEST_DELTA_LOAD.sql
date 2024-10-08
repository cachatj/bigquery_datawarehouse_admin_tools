
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_TEST_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for TEST
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.TEST_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TEST`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TEST` AS target
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
        TARGET.DEAREGISTRATIONNUMBER_DEA_REGISTRATION = SOURCE.DEAREGISTRATIONNUMBER_DEA_REGISTRATION,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.DEL_FLG = SOURCE.DEL_FLG,
        TARGET.VRSN_START_DTE = SOURCE.VRSN_START_DTE,
        TARGET.VRSN_END_DTE = SOURCE.VRSN_END_DTE,
        TARGET.PAYMENTINDICATOR_DEA_REGISTRATION = SOURCE.PAYMENTINDICATOR_DEA_REGISTRATION,
        TARGET.PAYMENTINDICATORDESC = SOURCE.PAYMENTINDICATORDESC,
        TARGET.BUSINESSACTIVITYCODE_DEA_REGISTRATION = SOURCE.BUSINESSACTIVITYCODE_DEA_REGISTRATION,
        TARGET.BUSINESSACTIVITYSUBCODE_DEA_REGISTRATION = SOURCE.BUSINESSACTIVITYSUBCODE_DEA_REGISTRATION,
        TARGET.BUSINESS_ACTIVITY_DESC_TXT_BUSINESS_ACTIVITY = SOURCE.BUSINESS_ACTIVITY_DESC_TXT_BUSINESS_ACTIVITY,
        TARGET.EXPIRATIONDATE_DEA_REGISTRATION = SOURCE.EXPIRATIONDATE_DEA_REGISTRATION,
        TARGET.NAME_DEA_REGISTRATION = SOURCE.NAME_DEA_REGISTRATION,
        TARGET.ADDITIONALCOMPANYINFO_DEA_REGISTRATION = SOURCE.ADDITIONALCOMPANYINFO_DEA_REGISTRATION,
        TARGET.ADDRESS1_DEA_REGISTRATION = SOURCE.ADDRESS1_DEA_REGISTRATION,
        TARGET.ADDRESS2_DEA_REGISTRATION = SOURCE.ADDRESS2_DEA_REGISTRATION,
        TARGET.CITY_DEA_REGISTRATION = SOURCE.CITY_DEA_REGISTRATION,
        TARGET.STATE_DEA_REGISTRATION = SOURCE.STATE_DEA_REGISTRATION,
        TARGET.CALC_DEA_SCHED_1_FLG = SOURCE.CALC_DEA_SCHED_1_FLG,
        TARGET.CALC_DEA_SCHED_2_FLG = SOURCE.CALC_DEA_SCHED_2_FLG,
        TARGET.CALC_DEA_SCHED_3_FLG = SOURCE.CALC_DEA_SCHED_3_FLG,
        TARGET.CALC_DEA_SCHED_2N_FLG = SOURCE.CALC_DEA_SCHED_2N_FLG,
        TARGET.CALC_DEA_SCHED_3N_FLG = SOURCE.CALC_DEA_SCHED_3N_FLG,
        TARGET.CALC_DEA_SCHED_4_FLG = SOURCE.CALC_DEA_SCHED_4_FLG,
        TARGET.CALC_DEA_SCHED_5_FLG = SOURCE.CALC_DEA_SCHED_5_FLG,
        TARGET.CALC_DEA_SCHED_L1_FLG = SOURCE.CALC_DEA_SCHED_L1_FLG,
        TARGET.ZIP_DEA_REGISTRATION = SOURCE.ZIP_DEA_REGISTRATION,
        TARGET.DEGREE_DEA_REGISTRATION = SOURCE.DEGREE_DEA_REGISTRATION,
        TARGET.STATELICENSENUMBER_DEA_REGISTRATION = SOURCE.STATELICENSENUMBER_DEA_REGISTRATION,
        TARGET.STATECSLICENSENUMBER_DEA_REGISTRATION = SOURCE.STATECSLICENSENUMBER_DEA_REGISTRATION,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.DEAREGISTRATIONNUMBER_DEA_REGISTRATION,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.DEL_FLG,
        SOURCE.VRSN_START_DTE,
        SOURCE.VRSN_END_DTE,
        SOURCE.PAYMENTINDICATOR_DEA_REGISTRATION,
        SOURCE.PAYMENTINDICATORDESC,
        SOURCE.BUSINESSACTIVITYCODE_DEA_REGISTRATION,
        SOURCE.BUSINESSACTIVITYSUBCODE_DEA_REGISTRATION,
        SOURCE.BUSINESS_ACTIVITY_DESC_TXT_BUSINESS_ACTIVITY,
        SOURCE.EXPIRATIONDATE_DEA_REGISTRATION,
        SOURCE.NAME_DEA_REGISTRATION,
        SOURCE.ADDITIONALCOMPANYINFO_DEA_REGISTRATION,
        SOURCE.ADDRESS1_DEA_REGISTRATION,
        SOURCE.ADDRESS2_DEA_REGISTRATION,
        SOURCE.CITY_DEA_REGISTRATION,
        SOURCE.STATE_DEA_REGISTRATION,
        SOURCE.CALC_DEA_SCHED_1_FLG,
        SOURCE.CALC_DEA_SCHED_2_FLG,
        SOURCE.CALC_DEA_SCHED_3_FLG,
        SOURCE.CALC_DEA_SCHED_2N_FLG,
        SOURCE.CALC_DEA_SCHED_3N_FLG,
        SOURCE.CALC_DEA_SCHED_4_FLG,
        SOURCE.CALC_DEA_SCHED_5_FLG,
        SOURCE.CALC_DEA_SCHED_L1_FLG,
        SOURCE.ZIP_DEA_REGISTRATION,
        SOURCE.DEGREE_DEA_REGISTRATION,
        SOURCE.STATELICENSENUMBER_DEA_REGISTRATION,
        SOURCE.STATECSLICENSENUMBER_DEA_REGISTRATION,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
