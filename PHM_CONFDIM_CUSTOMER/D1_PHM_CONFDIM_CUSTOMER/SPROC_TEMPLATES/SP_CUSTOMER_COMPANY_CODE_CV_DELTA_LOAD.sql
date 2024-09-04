
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_COMPANY_CODE_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for CUSTOMER_COMPANY_CODE_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_COMPANY_CODE_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_COMPANY_CODE_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_COMPANY_CODE_CV` AS target
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
        TARGET.KUNNR_KNB1 = SOURCE.KUNNR_KNB1,
        TARGET.BUKRS_KNB1 = SOURCE.BUKRS_KNB1,
        TARGET.PERNR_KNB1 = SOURCE.PERNR_KNB1,
        TARGET.ERDAT_KNB1 = SOURCE.ERDAT_KNB1,
        TARGET.ERNAM_KNB1 = SOURCE.ERNAM_KNB1,
        TARGET.SPERR_KNB1 = SOURCE.SPERR_KNB1,
        TARGET.LOEVM_KNB1 = SOURCE.LOEVM_KNB1,
        TARGET.ZUAWA_KNB1 = SOURCE.ZUAWA_KNB1,
        TARGET.BUSAB_KNB1 = SOURCE.BUSAB_KNB1,
        TARGET.AKONT_KNB1 = SOURCE.AKONT_KNB1,
        TARGET.BEGRU_KNB1 = SOURCE.BEGRU_KNB1,
        TARGET.KNRZE_KNB1 = SOURCE.KNRZE_KNB1,
        TARGET.KNRZB_KNB1 = SOURCE.KNRZB_KNB1,
        TARGET.ZAMIM_KNB1 = SOURCE.ZAMIM_KNB1,
        TARGET.ZAMIV_KNB1 = SOURCE.ZAMIV_KNB1,
        TARGET.ZAMIR_KNB1 = SOURCE.ZAMIR_KNB1,
        TARGET.ZAMIB_KNB1 = SOURCE.ZAMIB_KNB1,
        TARGET.ZAMIO_KNB1 = SOURCE.ZAMIO_KNB1,
        TARGET.ZWELS_KNB1 = SOURCE.ZWELS_KNB1,
        TARGET.XVERR_KNB1 = SOURCE.XVERR_KNB1,
        TARGET.ZAHLS_KNB1 = SOURCE.ZAHLS_KNB1,
        TARGET.ZTERM_KNB1 = SOURCE.ZTERM_KNB1,
        TARGET.WAKON_KNB1 = SOURCE.WAKON_KNB1,
        TARGET.VZSKZ_KNB1 = SOURCE.VZSKZ_KNB1,
        TARGET.ZINDT_KNB1 = SOURCE.ZINDT_KNB1,
        TARGET.ZINRT_KNB1 = SOURCE.ZINRT_KNB1,
        TARGET.EIKTO_KNB1 = SOURCE.EIKTO_KNB1,
        TARGET.ZSABE_KNB1 = SOURCE.ZSABE_KNB1,
        TARGET.KVERM_KNB1 = SOURCE.KVERM_KNB1,
        TARGET.FDGRV_KNB1 = SOURCE.FDGRV_KNB1,
        TARGET.VRBKZ_KNB1 = SOURCE.VRBKZ_KNB1,
        TARGET.VLIBB_KNB1 = SOURCE.VLIBB_KNB1,
        TARGET.VRSZL_KNB1 = SOURCE.VRSZL_KNB1,
        TARGET.VRSPR_KNB1 = SOURCE.VRSPR_KNB1,
        TARGET.VRSNR_KNB1 = SOURCE.VRSNR_KNB1,
        TARGET.VERDT_KNB1 = SOURCE.VERDT_KNB1,
        TARGET.PERKZ_KNB1 = SOURCE.PERKZ_KNB1,
        TARGET.XDEZV_KNB1 = SOURCE.XDEZV_KNB1,
        TARGET.XAUSZ_KNB1 = SOURCE.XAUSZ_KNB1,
        TARGET.WEBTR_KNB1 = SOURCE.WEBTR_KNB1,
        TARGET.REMIT_KNB1 = SOURCE.REMIT_KNB1,
        TARGET.DATLZ_KNB1 = SOURCE.DATLZ_KNB1,
        TARGET.XZVER_KNB1 = SOURCE.XZVER_KNB1,
        TARGET.TOGRU_KNB1 = SOURCE.TOGRU_KNB1,
        TARGET.KULTG_KNB1 = SOURCE.KULTG_KNB1,
        TARGET.HBKID_KNB1 = SOURCE.HBKID_KNB1,
        TARGET.XPORE_KNB1 = SOURCE.XPORE_KNB1,
        TARGET.BLNKZ_KNB1 = SOURCE.BLNKZ_KNB1,
        TARGET.ALTKN_KNB1 = SOURCE.ALTKN_KNB1,
        TARGET.ZGRUP_KNB1 = SOURCE.ZGRUP_KNB1,
        TARGET.URLID_KNB1 = SOURCE.URLID_KNB1,
        TARGET.MGRUP_KNB1 = SOURCE.MGRUP_KNB1,
        TARGET.LOCKB_KNB1 = SOURCE.LOCKB_KNB1,
        TARGET.UZAWE_KNB1 = SOURCE.UZAWE_KNB1,
        TARGET.EKVBD_KNB1 = SOURCE.EKVBD_KNB1,
        TARGET.SREGL_KNB1 = SOURCE.SREGL_KNB1,
        TARGET.XEDIP_KNB1 = SOURCE.XEDIP_KNB1,
        TARGET.FRGRP_KNB1 = SOURCE.FRGRP_KNB1,
        TARGET.VRSDG_KNB1 = SOURCE.VRSDG_KNB1,
        TARGET.TLFXS_KNB1 = SOURCE.TLFXS_KNB1,
        TARGET.INTAD_KNB1 = SOURCE.INTAD_KNB1,
        TARGET.XKNZB_KNB1 = SOURCE.XKNZB_KNB1,
        TARGET.GUZTE_KNB1 = SOURCE.GUZTE_KNB1,
        TARGET.GRICD_KNB1 = SOURCE.GRICD_KNB1,
        TARGET.GRIDT_KNB1 = SOURCE.GRIDT_KNB1,
        TARGET.WBRSL_KNB1 = SOURCE.WBRSL_KNB1,
        TARGET.CONFS_KNB1 = SOURCE.CONFS_KNB1,
        TARGET.UPDAT_KNB1 = SOURCE.UPDAT_KNB1,
        TARGET.UPTIM_KNB1 = SOURCE.UPTIM_KNB1,
        TARGET.NODEL_KNB1 = SOURCE.NODEL_KNB1,
        TARGET.TLFNS_KNB1 = SOURCE.TLFNS_KNB1,
        TARGET.CESSION_KZ_KNB1 = SOURCE.CESSION_KZ_KNB1,
        TARGET.AVSND_KNB1 = SOURCE.AVSND_KNB1,
        TARGET.AD_HASH_KNB1 = SOURCE.AD_HASH_KNB1,
        TARGET.QLAND_KNB1 = SOURCE.QLAND_KNB1,
        TARGET.CVP_XBLCK_B_KNB1 = SOURCE.CVP_XBLCK_B_KNB1,
        TARGET.GMVKZD_KNB1 = SOURCE.GMVKZD_KNB1,
        TARGET.YY_STMD_IND_KNB1 = SOURCE.YY_STMD_IND_KNB1,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.KUNNR_KNB1,
        SOURCE.BUKRS_KNB1,
        SOURCE.PERNR_KNB1,
        SOURCE.ERDAT_KNB1,
        SOURCE.ERNAM_KNB1,
        SOURCE.SPERR_KNB1,
        SOURCE.LOEVM_KNB1,
        SOURCE.ZUAWA_KNB1,
        SOURCE.BUSAB_KNB1,
        SOURCE.AKONT_KNB1,
        SOURCE.BEGRU_KNB1,
        SOURCE.KNRZE_KNB1,
        SOURCE.KNRZB_KNB1,
        SOURCE.ZAMIM_KNB1,
        SOURCE.ZAMIV_KNB1,
        SOURCE.ZAMIR_KNB1,
        SOURCE.ZAMIB_KNB1,
        SOURCE.ZAMIO_KNB1,
        SOURCE.ZWELS_KNB1,
        SOURCE.XVERR_KNB1,
        SOURCE.ZAHLS_KNB1,
        SOURCE.ZTERM_KNB1,
        SOURCE.WAKON_KNB1,
        SOURCE.VZSKZ_KNB1,
        SOURCE.ZINDT_KNB1,
        SOURCE.ZINRT_KNB1,
        SOURCE.EIKTO_KNB1,
        SOURCE.ZSABE_KNB1,
        SOURCE.KVERM_KNB1,
        SOURCE.FDGRV_KNB1,
        SOURCE.VRBKZ_KNB1,
        SOURCE.VLIBB_KNB1,
        SOURCE.VRSZL_KNB1,
        SOURCE.VRSPR_KNB1,
        SOURCE.VRSNR_KNB1,
        SOURCE.VERDT_KNB1,
        SOURCE.PERKZ_KNB1,
        SOURCE.XDEZV_KNB1,
        SOURCE.XAUSZ_KNB1,
        SOURCE.WEBTR_KNB1,
        SOURCE.REMIT_KNB1,
        SOURCE.DATLZ_KNB1,
        SOURCE.XZVER_KNB1,
        SOURCE.TOGRU_KNB1,
        SOURCE.KULTG_KNB1,
        SOURCE.HBKID_KNB1,
        SOURCE.XPORE_KNB1,
        SOURCE.BLNKZ_KNB1,
        SOURCE.ALTKN_KNB1,
        SOURCE.ZGRUP_KNB1,
        SOURCE.URLID_KNB1,
        SOURCE.MGRUP_KNB1,
        SOURCE.LOCKB_KNB1,
        SOURCE.UZAWE_KNB1,
        SOURCE.EKVBD_KNB1,
        SOURCE.SREGL_KNB1,
        SOURCE.XEDIP_KNB1,
        SOURCE.FRGRP_KNB1,
        SOURCE.VRSDG_KNB1,
        SOURCE.TLFXS_KNB1,
        SOURCE.INTAD_KNB1,
        SOURCE.XKNZB_KNB1,
        SOURCE.GUZTE_KNB1,
        SOURCE.GRICD_KNB1,
        SOURCE.GRIDT_KNB1,
        SOURCE.WBRSL_KNB1,
        SOURCE.CONFS_KNB1,
        SOURCE.UPDAT_KNB1,
        SOURCE.UPTIM_KNB1,
        SOURCE.NODEL_KNB1,
        SOURCE.TLFNS_KNB1,
        SOURCE.CESSION_KZ_KNB1,
        SOURCE.AVSND_KNB1,
        SOURCE.AD_HASH_KNB1,
        SOURCE.QLAND_KNB1,
        SOURCE.CVP_XBLCK_B_KNB1,
        SOURCE.GMVKZD_KNB1,
        SOURCE.YY_STMD_IND_KNB1,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID
      );
    END;
