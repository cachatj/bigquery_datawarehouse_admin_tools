
    CREATE PROCEDURE  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SP_CHPC_MBR_ACCESS_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CHPC_MBR_ACCESS
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
    SET v_stored_proc_name = 'D2_PHM_CONFFACT_PRICING.CHPC_MBR_ACCESS_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_MBR_ACCESS`);


    MERGE INTO  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_MBR_ACCESS` AS target
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
        TARGET.root_affiliation_number = SOURCE.root_affiliation_number,
        TARGET.root_affiliation_name = SOURCE.root_affiliation_name,
        TARGET.affiliation_number = SOURCE.affiliation_number,
        TARGET.affiliation_name = SOURCE.affiliation_name,
        TARGET.group_id = SOURCE.group_id,
        TARGET.ship_to_customer_number = SOURCE.ship_to_customer_number,
        TARGET.ship_to_customer_name = SOURCE.ship_to_customer_name,
        TARGET.group_number = SOURCE.group_number,
        TARGET.group_name = SOURCE.group_name,
        TARGET.group_priority_ranking = SOURCE.group_priority_ranking,
        TARGET.secondary_priority_ranking = SOURCE.secondary_priority_ranking,
        TARGET.account_status_flag = SOURCE.account_status_flag,
        TARGET.relationship_status_flag = SOURCE.relationship_status_flag,
        TARGET.relationship_effective_date = SOURCE.relationship_effective_date,
        TARGET.edi_type_code = SOURCE.edi_type_code,
        TARGET.record_effective_date = SOURCE.record_effective_date,
        TARGET.A2_root_affiliation_number = SOURCE.A2_root_affiliation_number,
        TARGET.A2_root_affiliation_name = SOURCE.A2_root_affiliation_name
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.root_affiliation_number,
        SOURCE.root_affiliation_name,
        SOURCE.affiliation_number,
        SOURCE.affiliation_name,
        SOURCE.group_id,
        SOURCE.ship_to_customer_number,
        SOURCE.ship_to_customer_name,
        SOURCE.group_number,
        SOURCE.group_name,
        SOURCE.group_priority_ranking,
        SOURCE.secondary_priority_ranking,
        SOURCE.account_status_flag,
        SOURCE.relationship_status_flag,
        SOURCE.relationship_effective_date,
        SOURCE.edi_type_code,
        SOURCE.record_effective_date,
        SOURCE.A2_root_affiliation_number,
        SOURCE.A2_root_affiliation_name
      );
    END;
