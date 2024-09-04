
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_DW_LOG.SP_ADMIN_GWSA_COLUMN_SECURITY_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for ADMIN_GWSA_COLUMN_SECURITY_CV
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
    SET v_stored_proc_name = 'D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_SECURITY_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_SECURITY_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_GWSA_COLUMN_SECURITY_CV` AS target
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
        TARGET.BUSINESS_REVIEW = SOURCE.BUSINESS_REVIEW,
        TARGET.ADMIN_ENTITY_INVENTORY_ENTITY_SCHEMA = SOURCE.ADMIN_ENTITY_INVENTORY_ENTITY_SCHEMA,
        TARGET.ADMIN_ENTITY_INVENTORY_ENTITY_NAME = SOURCE.ADMIN_ENTITY_INVENTORY_ENTITY_NAME,
        TARGET.PDW_COLUMNS_DATA_TYPE = SOURCE.PDW_COLUMNS_DATA_TYPE,
        TARGET.D1_COLUMN_NAME = SOURCE.D1_COLUMN_NAME,
        TARGET.VW_COLUMN_NAME = SOURCE.VW_COLUMN_NAME,
        TARGET.LOOKER_COLUMN_NAME = SOURCE.LOOKER_COLUMN_NAME,
        TARGET.GwsaPubRedOak = SOURCE.GwsaPubRedOak,
        TARGET.GwsaPubOosSrc = SOURCE.GwsaPubOosSrc,
        TARGET.GwsaPubPricing = SOURCE.GwsaPubPricing,
        TARGET.GwsaPublished = SOURCE.GwsaPublished,
        TARGET.GwsaOperational = SOURCE.GwsaOperational
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.BUSINESS_REVIEW,
        SOURCE.ADMIN_ENTITY_INVENTORY_ENTITY_SCHEMA,
        SOURCE.ADMIN_ENTITY_INVENTORY_ENTITY_NAME,
        SOURCE.PDW_COLUMNS_DATA_TYPE,
        SOURCE.D1_COLUMN_NAME,
        SOURCE.VW_COLUMN_NAME,
        SOURCE.LOOKER_COLUMN_NAME,
        SOURCE.GwsaPubRedOak,
        SOURCE.GwsaPubOosSrc,
        SOURCE.GwsaPubPricing,
        SOURCE.GwsaPublished,
        SOURCE.GwsaOperational
      );
    END;
