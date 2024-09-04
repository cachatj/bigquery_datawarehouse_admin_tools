
    CREATE PROCEDURE  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SP_CHPC_BCKORD_TEMP_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CHPC_BCKORD_TEMP
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
    SET v_stored_proc_name = 'D2_PHM_CONFFACT_PRICING.CHPC_BCKORD_TEMP_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_BCKORD_TEMP`);


    MERGE INTO  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_BCKORD_TEMP` AS target
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
        TARGET.cardinal_substitution_key = SOURCE.cardinal_substitution_key,
        TARGET.vendor_name = SOURCE.vendor_name,
        TARGET.corporate_item_number = SOURCE.corporate_item_number,
        TARGET.national_drug_code = SOURCE.national_drug_code,
        TARGET.corporate_description = SOURCE.corporate_description,
        TARGET.AAAITEM = SOURCE.AAAITEM,
        TARGET.availability_alert_code = SOURCE.availability_alert_code,
        TARGET.external_message_code = SOURCE.external_message_code,
        TARGET.external_message = SOURCE.external_message,
        TARGET.expected_delivery_date = SOURCE.expected_delivery_date,
        TARGET.internal_message = SOURCE.internal_message,
        TARGET.change_date = SOURCE.change_date,
        TARGET.origination_date = SOURCE.origination_date,
        TARGET.expediting_code_type = SOURCE.expediting_code_type
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.cardinal_substitution_key,
        SOURCE.vendor_name,
        SOURCE.corporate_item_number,
        SOURCE.national_drug_code,
        SOURCE.corporate_description,
        SOURCE.AAAITEM,
        SOURCE.availability_alert_code,
        SOURCE.external_message_code,
        SOURCE.external_message,
        SOURCE.expected_delivery_date,
        SOURCE.internal_message,
        SOURCE.change_date,
        SOURCE.origination_date,
        SOURCE.expediting_code_type
      );
    END;
