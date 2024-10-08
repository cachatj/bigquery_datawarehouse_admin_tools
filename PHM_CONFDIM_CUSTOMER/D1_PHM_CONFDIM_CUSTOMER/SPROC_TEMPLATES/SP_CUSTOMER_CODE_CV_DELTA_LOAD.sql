
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_CODE_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CUSTOMER_CODE_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_CODE_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_CODE_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_CODE_CV` AS target
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
        TARGET.KUNNR_YTSD_CUST_CODE = SOURCE.KUNNR_YTSD_CUST_CODE,
        TARGET.VKORG_YTSD_CUST_CODE = SOURCE.VKORG_YTSD_CUST_CODE,
        TARGET.VTWEG_YTSD_CUST_CODE = SOURCE.VTWEG_YTSD_CUST_CODE,
        TARGET.SPART_YTSD_CUST_CODE = SOURCE.SPART_YTSD_CUST_CODE,
        TARGET.YYCODETYPE_YTSD_CUST_CODE = SOURCE.YYCODETYPE_YTSD_CUST_CODE,
        TARGET.YYCODETYPE_DES_YTSD_CODETYPETXT = SOURCE.YYCODETYPE_DES_YTSD_CODETYPETXT,
        TARGET.YYCODENUM_YTSD_CUST_CODE = SOURCE.YYCODENUM_YTSD_CUST_CODE,
        TARGET.YYCODENAME_YTSD_CODENUMTXT = SOURCE.YYCODENAME_YTSD_CODENUMTXT,
        TARGET.YYCODEVALNUM_YTSD_CUST_CODE = SOURCE.YYCODEVALNUM_YTSD_CUST_CODE,
        TARGET.YYCODESTRTDTE_YTSD_CUST_CODE = SOURCE.YYCODESTRTDTE_YTSD_CUST_CODE,
        TARGET.YYCODEENDDTE_YTSD_CUST_CODE = SOURCE.YYCODEENDDTE_YTSD_CUST_CODE,
        TARGET.YYCODECMNTS_YTSD_CUST_CODE = SOURCE.YYCODECMNTS_YTSD_CUST_CODE,
        TARGET.YYCODESOURCE_YTSD_CUST_CODE = SOURCE.YYCODESOURCE_YTSD_CUST_CODE,
        TARGET.UPDKZ_YTSD_CUST_CODE = SOURCE.UPDKZ_YTSD_CUST_CODE,
        TARGET.CREATED_BY_YTSD_CUST_CODE = SOURCE.CREATED_BY_YTSD_CUST_CODE,
        TARGET.CREATED_ON_YTSD_CUST_CODE = SOURCE.CREATED_ON_YTSD_CUST_CODE,
        TARGET.CHANGED_BY_YTSD_CUST_CODE = SOURCE.CHANGED_BY_YTSD_CUST_CODE,
        TARGET.CHANGED_ON_YTSD_CUST_CODE = SOURCE.CHANGED_ON_YTSD_CUST_CODE,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.KUNNR_YTSD_CUST_CODE,
        SOURCE.VKORG_YTSD_CUST_CODE,
        SOURCE.VTWEG_YTSD_CUST_CODE,
        SOURCE.SPART_YTSD_CUST_CODE,
        SOURCE.YYCODETYPE_YTSD_CUST_CODE,
        SOURCE.YYCODETYPE_DES_YTSD_CODETYPETXT,
        SOURCE.YYCODENUM_YTSD_CUST_CODE,
        SOURCE.YYCODENAME_YTSD_CODENUMTXT,
        SOURCE.YYCODEVALNUM_YTSD_CUST_CODE,
        SOURCE.YYCODESTRTDTE_YTSD_CUST_CODE,
        SOURCE.YYCODEENDDTE_YTSD_CUST_CODE,
        SOURCE.YYCODECMNTS_YTSD_CUST_CODE,
        SOURCE.YYCODESOURCE_YTSD_CUST_CODE,
        SOURCE.UPDKZ_YTSD_CUST_CODE,
        SOURCE.CREATED_BY_YTSD_CUST_CODE,
        SOURCE.CREATED_ON_YTSD_CUST_CODE,
        SOURCE.CHANGED_BY_YTSD_CUST_CODE,
        SOURCE.CHANGED_ON_YTSD_CUST_CODE,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
