
    CREATE PROCEDURE  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SP_WAC_DATA_TEMP_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for WAC_DATA_TEMP
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
    SET v_stored_proc_name = 'D2_PHM_CONFFACT_PRICING.WAC_DATA_TEMP_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.WAC_DATA_TEMP`);


    MERGE INTO  `PROJECT_ID.D2_PHM_CONFFACT_PRICING.WAC_DATA_TEMP` AS target
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
        TARGET.national_drug_code = SOURCE.national_drug_code,
        TARGET.new_awp = SOURCE.new_awp,
        TARGET.new_corp_nifo = SOURCE.new_corp_nifo,
        TARGET.new_cost_plus = SOURCE.new_cost_plus,
        TARGET.new_base_cost = SOURCE.new_base_cost,
        TARGET.new_additional_charge = SOURCE.new_additional_charge,
        TARGET.effective_date = SOURCE.effective_date,
        TARGET.activity_status_code = SOURCE.activity_status_code,
        TARGET.activity_description = SOURCE.activity_description,
        TARGET.generic_indicator_code = SOURCE.generic_indicator_code,
        TARGET.cardinal_substitution_key = SOURCE.cardinal_substitution_key,
        TARGET.cardinal_gcn_code = SOURCE.cardinal_gcn_code,
        TARGET.corporate_item_number = SOURCE.corporate_item_number,
        TARGET.hamacher_department_number = SOURCE.hamacher_department_number,
        TARGET.hamacher_description = SOURCE.hamacher_description,
        TARGET.form_id = SOURCE.form_id,
        TARGET.form_description = SOURCE.form_description,
        TARGET.multi_source_indicator_code = SOURCE.multi_source_indicator_code,
        TARGET.package_quantity = SOURCE.package_quantity,
        TARGET.Accunet_Size = SOURCE.Accunet_Size,
        TARGET.package_size_unit = SOURCE.package_size_unit,
        TARGET.package_size_description = SOURCE.package_size_description,
        TARGET.unit_code = SOURCE.unit_code,
        TARGET.Unit_Code_Desc = SOURCE.Unit_Code_Desc,
        TARGET.Private_Label_Code = SOURCE.Private_Label_Code,
        TARGET.Private_Label_Desc = SOURCE.Private_Label_Desc,
        TARGET.Package_Size = SOURCE.Package_Size,
        TARGET.Strength = SOURCE.Strength,
        TARGET.Total_Quantity = SOURCE.Total_Quantity,
        TARGET.Trade_Name = SOURCE.Trade_Name,
        TARGET.Unit_Dose = SOURCE.Unit_Dose,
        TARGET.Unit_Dose_Desc = SOURCE.Unit_Dose_Desc,
        TARGET.DRUG_GROUP_ID = SOURCE.DRUG_GROUP_ID,
        TARGET.DESC_TXT = SOURCE.DESC_TXT,
        TARGET.Vendor_Number = SOURCE.Vendor_Number,
        TARGET.Vendor_Name = SOURCE.Vendor_Name,
        TARGET.DEA_Schedule_Number = SOURCE.DEA_Schedule_Number,
        TARGET.deaSchedDesc = SOURCE.deaSchedDesc,
        TARGET.Item_Type_Code = SOURCE.Item_Type_Code,
        TARGET.Item_Type_Code_Description = SOURCE.Item_Type_Code_Description,
        TARGET.Department_Code = SOURCE.Department_Code,
        TARGET.department_description = SOURCE.department_description,
        TARGET.caseQty = SOURCE.caseQty,
        TARGET.ahfs_description = SOURCE.ahfs_description,
        TARGET.route_administration = SOURCE.route_administration,
        TARGET.minPackQty = SOURCE.minPackQty,
        TARGET.generic_name = SOURCE.generic_name,
        TARGET.orange_book_code = SOURCE.orange_book_code,
        TARGET.hicl_seq_number = SOURCE.hicl_seq_number,
        TARGET.hicl = SOURCE.hicl,
        TARGET.totalCaseQty = SOURCE.totalCaseQty,
        TARGET.recordDate = SOURCE.recordDate,
        TARGET.rxIndicator = SOURCE.rxIndicator,
        TARGET.prItem = SOURCE.prItem,
        TARGET.materialType = SOURCE.materialType,
        TARGET.materialGroup = SOURCE.materialGroup,
        TARGET.materialPackageGroup = SOURCE.materialPackageGroup,
        TARGET.materialGroupCategory = SOURCE.materialGroupCategory,
        TARGET.VIZIENT_ITM = SOURCE.VIZIENT_ITM,
        TARGET.PREMIERPRORX_ITM = SOURCE.PREMIERPRORX_ITM
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.national_drug_code,
        SOURCE.new_awp,
        SOURCE.new_corp_nifo,
        SOURCE.new_cost_plus,
        SOURCE.new_base_cost,
        SOURCE.new_additional_charge,
        SOURCE.effective_date,
        SOURCE.activity_status_code,
        SOURCE.activity_description,
        SOURCE.generic_indicator_code,
        SOURCE.cardinal_substitution_key,
        SOURCE.cardinal_gcn_code,
        SOURCE.corporate_item_number,
        SOURCE.hamacher_department_number,
        SOURCE.hamacher_description,
        SOURCE.form_id,
        SOURCE.form_description,
        SOURCE.multi_source_indicator_code,
        SOURCE.package_quantity,
        SOURCE.Accunet_Size,
        SOURCE.package_size_unit,
        SOURCE.package_size_description,
        SOURCE.unit_code,
        SOURCE.Unit_Code_Desc,
        SOURCE.Private_Label_Code,
        SOURCE.Private_Label_Desc,
        SOURCE.Package_Size,
        SOURCE.Strength,
        SOURCE.Total_Quantity,
        SOURCE.Trade_Name,
        SOURCE.Unit_Dose,
        SOURCE.Unit_Dose_Desc,
        SOURCE.DRUG_GROUP_ID,
        SOURCE.DESC_TXT,
        SOURCE.Vendor_Number,
        SOURCE.Vendor_Name,
        SOURCE.DEA_Schedule_Number,
        SOURCE.deaSchedDesc,
        SOURCE.Item_Type_Code,
        SOURCE.Item_Type_Code_Description,
        SOURCE.Department_Code,
        SOURCE.department_description,
        SOURCE.caseQty,
        SOURCE.ahfs_description,
        SOURCE.route_administration,
        SOURCE.minPackQty,
        SOURCE.generic_name,
        SOURCE.orange_book_code,
        SOURCE.hicl_seq_number,
        SOURCE.hicl,
        SOURCE.totalCaseQty,
        SOURCE.recordDate,
        SOURCE.rxIndicator,
        SOURCE.prItem,
        SOURCE.materialType,
        SOURCE.materialGroup,
        SOURCE.materialPackageGroup,
        SOURCE.materialGroupCategory,
        SOURCE.VIZIENT_ITM,
        SOURCE.PREMIERPRORX_ITM
      );
    END;
