
    CREATE PROCEDURE  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SP_GRP_CURPRC_TEMP_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for GRP_CURPRC_TEMP
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
    SET v_stored_proc_name = 'D2_PHM_CONFFACT_PRICING.GRP_CURPRC_TEMP_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.GRP_CURPRC_TEMP`);


    MERGE INTO  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.GRP_CURPRC_TEMP` AS target
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
        TARGET.group_number = SOURCE.group_number,
        TARGET.group_name = SOURCE.group_name,
        TARGET.group_type_id = SOURCE.group_type_id,
        TARGET.contract_description = SOURCE.contract_description,
        TARGET.group_reference = SOURCE.group_reference,
        TARGET.contract_number = SOURCE.contract_number,
        TARGET.contract_effective_year = SOURCE.contract_effective_year,
        TARGET.contract_effective_month = SOURCE.contract_effective_month,
        TARGET.contract_effective_date = SOURCE.contract_effective_date,
        TARGET.contract_term_year = SOURCE.contract_term_year,
        TARGET.contract_term_month = SOURCE.contract_term_month,
        TARGET.contract_term_date = SOURCE.contract_term_date,
        TARGET.vendor_number = SOURCE.vendor_number,
        TARGET.vendor_name = SOURCE.vendor_name,
        TARGET.parent_vendor_number = SOURCE.parent_vendor_number,
        TARGET.parent_vendor_name = SOURCE.parent_vendor_name,
        TARGET.corporate_item_number = SOURCE.corporate_item_number,
        TARGET.corporate_description = SOURCE.corporate_description,
        TARGET.national_drug_code = SOURCE.national_drug_code,
        TARGET.contract_cost = SOURCE.contract_cost,
        TARGET.contract_sell = SOURCE.contract_sell,
        TARGET.parent_affiliation_id = SOURCE.parent_affiliation_id,
        TARGET.parent_affiliation = SOURCE.parent_affiliation,
        TARGET.vendor_reference = SOURCE.vendor_reference,
        TARGET.item_effective_date = SOURCE.item_effective_date,
        TARGET.record_effective_date = SOURCE.record_effective_date
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.group_number,
        SOURCE.group_name,
        SOURCE.group_type_id,
        SOURCE.contract_description,
        SOURCE.group_reference,
        SOURCE.contract_number,
        SOURCE.contract_effective_year,
        SOURCE.contract_effective_month,
        SOURCE.contract_effective_date,
        SOURCE.contract_term_year,
        SOURCE.contract_term_month,
        SOURCE.contract_term_date,
        SOURCE.vendor_number,
        SOURCE.vendor_name,
        SOURCE.parent_vendor_number,
        SOURCE.parent_vendor_name,
        SOURCE.corporate_item_number,
        SOURCE.corporate_description,
        SOURCE.national_drug_code,
        SOURCE.contract_cost,
        SOURCE.contract_sell,
        SOURCE.parent_affiliation_id,
        SOURCE.parent_affiliation,
        SOURCE.vendor_reference,
        SOURCE.item_effective_date,
        SOURCE.record_effective_date
      );
    END;
