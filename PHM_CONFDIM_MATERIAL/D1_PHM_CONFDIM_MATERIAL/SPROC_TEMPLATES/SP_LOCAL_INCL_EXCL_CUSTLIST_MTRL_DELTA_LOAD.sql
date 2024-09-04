
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SP_LOCAL_INCL_EXCL_CUSTLIST_MTRL_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for LOCAL_INCL_EXCL_CUSTLIST_MTRL
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL` AS target
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
        TARGET.YYVKORG_KOTG753 = SOURCE.YYVKORG_KOTG753,
        TARGET.YYVTEXT_TVKOT = SOURCE.YYVTEXT_TVKOT,
        TARGET.YYVTWEG_KOTG753 = SOURCE.YYVTWEG_KOTG753,
        TARGET.YYVTEXT_TVTWT = SOURCE.YYVTEXT_TVTWT,
        TARGET.SPART_KOTG753 = SOURCE.SPART_KOTG753,
        TARGET.VTEXT_TSPAT = SOURCE.VTEXT_TSPAT,
        TARGET.IRM_CLNUM_KOTG753 = SOURCE.IRM_CLNUM_KOTG753,
        TARGET.DATAB_KOTG753 = SOURCE.DATAB_KOTG753,
        TARGET.DATBI_KOTG753 = SOURCE.DATBI_KOTG753,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID,
        TARGET.MATNR_KOTG753 = SOURCE.MATNR_KOTG753
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.YYVKORG_KOTG753,
        SOURCE.YYVTEXT_TVKOT,
        SOURCE.YYVTWEG_KOTG753,
        SOURCE.YYVTEXT_TVTWT,
        SOURCE.SPART_KOTG753,
        SOURCE.VTEXT_TSPAT,
        SOURCE.IRM_CLNUM_KOTG753,
        SOURCE.DATAB_KOTG753,
        SOURCE.DATBI_KOTG753,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID,
        SOURCE.MATNR_KOTG753
      );
    END;
