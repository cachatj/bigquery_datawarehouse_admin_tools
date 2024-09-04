
    CREATE PROCEDURE  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SP_VENDER_BLOCK_TEMP_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for VENDER_BLOCK_TEMP
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
    SET v_stored_proc_name = 'D2_PHM_CONFFACT_PRICING.VENDER_BLOCK_TEMP_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.VENDER_BLOCK_TEMP`);


    MERGE INTO  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.VENDER_BLOCK_TEMP` AS target
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
        TARGET.division_number = SOURCE.division_number,
        TARGET.ship_to_customer_number = SOURCE.ship_to_customer_number,
        TARGET.group_number = SOURCE.group_number,
        TARGET.vendor_number = SOURCE.vendor_number,
        TARGET.relationship_status = SOURCE.relationship_status,
        TARGET.relationship_effective_date = SOURCE.relationship_effective_date,
        TARGET.record_effective_date = SOURCE.record_effective_date,
        TARGET.Contract_Number = SOURCE.Contract_Number
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.division_number,
        SOURCE.ship_to_customer_number,
        SOURCE.group_number,
        SOURCE.vendor_number,
        SOURCE.relationship_status,
        SOURCE.relationship_effective_date,
        SOURCE.record_effective_date,
        SOURCE.Contract_Number
      );
    END;
