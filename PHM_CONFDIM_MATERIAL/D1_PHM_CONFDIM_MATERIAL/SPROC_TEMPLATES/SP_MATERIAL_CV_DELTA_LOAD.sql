
    CREATE PROCEDURE  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_CV_DELTA_LOAD`()
    BEGIN
    /***********************************************************************************************************************

    Author: <Automatically Written Using Code Generator>
    Creation Date: <Insert Date>
    Sproc Purpose: Delta Loading Merge Sproc for MATERIAL_CV
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
    SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.MATERIAL_CV_DELTA_LOAD';
    SET last_load_timestamp = (SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_CV`);


    MERGE INTO  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_CV` AS target
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
        TARGET.MATNR_MARA = SOURCE.MATNR_MARA,
        TARGET.VRSN_START_DTE = SOURCE.VRSN_START_DTE,
        TARGET.VRSN_END_DTE = SOURCE.VRSN_END_DTE,
        TARGET.CURR_VRSN_FLG = SOURCE.CURR_VRSN_FLG,
        TARGET.MAKTX_MAKT = SOURCE.MAKTX_MAKT,
        TARGET.CALC_ERSDA_MARA = SOURCE.CALC_ERSDA_MARA,
        TARGET.ERNAM_MARA = SOURCE.ERNAM_MARA,
        TARGET.LAST_CHG_DTE = SOURCE.LAST_CHG_DTE,
        TARGET.AENAM_MARA = SOURCE.AENAM_MARA,
        TARGET.LVORM_MARA = SOURCE.LVORM_MARA,
        TARGET.MTART_MARA = SOURCE.MTART_MARA,
        TARGET.MTBEZ_T134T_MATERIAL_TYPE = SOURCE.MTBEZ_T134T_MATERIAL_TYPE,
        TARGET.MBRSH_MARA = SOURCE.MBRSH_MARA,
        TARGET.MBBEZ_T137T_INDUSTRY_SECTOR = SOURCE.MBBEZ_T137T_INDUSTRY_SECTOR,
        TARGET.MATKL_MARA = SOURCE.MATKL_MARA,
        TARGET.WGBEZ_T023T_MATERIAL_GROUP = SOURCE.WGBEZ_T023T_MATERIAL_GROUP,
        TARGET.MEINS_MARA = SOURCE.MEINS_MARA,
        TARGET.BASE_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.BASE_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.BSTME_MARA = SOURCE.BSTME_MARA,
        TARGET.PURCH_ORDER_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.PURCH_ORDER_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.GROES_MARA = SOURCE.GROES_MARA,
        TARGET.BRGEW_MARA = SOURCE.BRGEW_MARA,
        TARGET.NTGEW_MARA = SOURCE.NTGEW_MARA,
        TARGET.GEWEI_MARA = SOURCE.GEWEI_MARA,
        TARGET.WEIGHT_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.WEIGHT_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.VOLUM_MARA = SOURCE.VOLUM_MARA,
        TARGET.VOLEH_MARA = SOURCE.VOLEH_MARA,
        TARGET.VOLUME_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.VOLUME_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.RAUBE_MARA = SOURCE.RAUBE_MARA,
        TARGET.RBTXT_T142T_STORAGE_CONDITION = SOURCE.RBTXT_T142T_STORAGE_CONDITION,
        TARGET.TEMPB_MARA = SOURCE.TEMPB_MARA,
        TARGET.TBTXT_T143T_TEMPERATURE_CONDITION = SOURCE.TBTXT_T143T_TEMPERATURE_CONDITION,
        TARGET.CALC_REFRIG_FLG = SOURCE.CALC_REFRIG_FLG,
        TARGET.DISST_MARA = SOURCE.DISST_MARA,
        TARGET.TRAGR_MARA = SOURCE.TRAGR_MARA,
        TARGET.VTEXT_TTGRT_TRANSPORTATION_GROUP = SOURCE.VTEXT_TTGRT_TRANSPORTATION_GROUP,
        TARGET.SPART_MARA = SOURCE.SPART_MARA,
        TARGET.CALC_NDC_ID = SOURCE.CALC_NDC_ID,
        TARGET.CALC_NDC_CTGRY_ID = SOURCE.CALC_NDC_CTGRY_ID,
        TARGET.CALC_EACH_UPC_ID = SOURCE.CALC_EACH_UPC_ID,
        TARGET.CALC_EACH_UPC_CTGRY_ID = SOURCE.CALC_EACH_UPC_CTGRY_ID,
        TARGET.CALC_CSE_UPC_ID = SOURCE.CALC_CSE_UPC_ID,
        TARGET.CALC_CSE_UPC_CTGRY_ID = SOURCE.CALC_CSE_UPC_CTGRY_ID,
        TARGET.CALC_CTN_UPC_ID = SOURCE.CALC_CTN_UPC_ID,
        TARGET.CALC_CTN_UPC_CTGRY_ID = SOURCE.CALC_CTN_UPC_CTGRY_ID,
        TARGET.CALC_EACH_EAN_ID = SOURCE.CALC_EACH_EAN_ID,
        TARGET.CALC_CSE_EAN_ID = SOURCE.CALC_CSE_EAN_ID,
        TARGET.CALC_CTN_EAN_ID = SOURCE.CALC_CTN_EAN_ID,
        TARGET.CALC_EACH_GTIN_ID = SOURCE.CALC_EACH_GTIN_ID,
        TARGET.CALC_CSE_GTIN_ID = SOURCE.CALC_CSE_GTIN_ID,
        TARGET.CALC_CTN_GTIN_ID = SOURCE.CALC_CTN_GTIN_ID,
        TARGET.LAENG_MARA = SOURCE.LAENG_MARA,
        TARGET.BREIT_MARA = SOURCE.BREIT_MARA,
        TARGET.HOEHE_MARA = SOURCE.HOEHE_MARA,
        TARGET.MEABM_MARA = SOURCE.MEABM_MARA,
        TARGET.DIM_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.DIM_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.PRDHA_MARA = SOURCE.PRDHA_MARA,
        TARGET.VTEXT_T179T_PRODUCT_HIERARCHY = SOURCE.VTEXT_T179T_PRODUCT_HIERARCHY,
        TARGET.VABME_MARA = SOURCE.VABME_MARA,
        TARGET.VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT = SOURCE.VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT,
        TARGET.VHART_MARA = SOURCE.VHART_MARA,
        TARGET.VTEXT_TVTYT_PACKAGING_MATERIAL_TYPE = SOURCE.VTEXT_TVTYT_PACKAGING_MATERIAL_TYPE,
        TARGET.MAGRV_MARA = SOURCE.MAGRV_MARA,
        TARGET.BEZEI_TVEGRT_MTRL_GRP_PACKAGING_MATERIALS = SOURCE.BEZEI_TVEGRT_MTRL_GRP_PACKAGING_MATERIALS,
        TARGET.EXTWG_MARA = SOURCE.EXTWG_MARA,
        TARGET.MSTAE_MARA = SOURCE.MSTAE_MARA,
        TARGET.MTSTB_T141T_CROSS_PLANT_MATERIAL_STATUS = SOURCE.MTSTB_T141T_CROSS_PLANT_MATERIAL_STATUS,
        TARGET.MSTAV_MARA = SOURCE.MSTAV_MARA,
        TARGET.VMSTB_TVMST_CROSS_DISTRIB_CHAIN_MTRL_STATUS = SOURCE.VMSTB_TVMST_CROSS_DISTRIB_CHAIN_MTRL_STATUS,
        TARGET.MSTDE_MARA = SOURCE.MSTDE_MARA,
        TARGET.MSTDV_MARA = SOURCE.MSTDV_MARA,
        TARGET.MHDRZ_MARA = SOURCE.MHDRZ_MARA,
        TARGET.KZUMW_MARA = SOURCE.KZUMW_MARA,
        TARGET.KOSCH_MARA = SOURCE.KOSCH_MARA,
        TARGET.VTEXT_T190ST_PRODUCT_ALLOCATION = SOURCE.VTEXT_T190ST_PRODUCT_ALLOCATION,
        TARGET.MFRPN_MARA = SOURCE.MFRPN_MARA,
        TARGET.MFRNR_MARA = SOURCE.MFRNR_MARA,
        TARGET.PROFL_MARA = SOURCE.PROFL_MARA,
        TARGET.PROFLD_TDG42_DANGEROUS_GOODS_INDICATOR = SOURCE.PROFLD_TDG42_DANGEROUS_GOODS_INDICATOR,
        TARGET.MTPOS_MARA_MARA = SOURCE.MTPOS_MARA_MARA,
        TARGET.BEZEI_TPTMT_ITEM_ITEM_CATEGORY_GROUP = SOURCE.BEZEI_TPTMT_ITEM_ITEM_CATEGORY_GROUP,
        TARGET.SLED_BBD_MARA = SOURCE.SLED_BBD_MARA,
        TARGET.EXP_DTE_IND_DESC_TXT = SOURCE.EXP_DTE_IND_DESC_TXT,
        TARGET.CALC_GEN_OTC_FLG = SOURCE.CALC_GEN_OTC_FLG,
        TARGET.CALC_RX_FLG = SOURCE.CALC_RX_FLG,
        TARGET.CALC_DEA_SCHED_ID = SOURCE.CALC_DEA_SCHED_ID,
        TARGET.BEZEI_T606V_LEGAL_CONTROL = SOURCE.BEZEI_T606V_LEGAL_CONTROL,
        TARGET.YYBASE_MARA = SOURCE.YYBASE_MARA,
        TARGET.YYDESC_YTMM_DEA_TEXT_DEA_BASE_CODE = SOURCE.YYDESC_YTMM_DEA_TEXT_DEA_BASE_CODE,
        TARGET.YYSUBB_MARA = SOURCE.YYSUBB_MARA,
        TARGET.YYDESC_YTMM_SUBBASETEXT_DEA_SUB_BASE_CODE = SOURCE.YYDESC_YTMM_SUBBASETEXT_DEA_SUB_BASE_CODE,
        TARGET.YYLDESC_MARA = SOURCE.YYLDESC_MARA,
        TARGET.YYAHFS_MARA = SOURCE.YYAHFS_MARA,
        TARGET.YYDESC_YTMM_AHFS_TEXT_AHFS_CODE = SOURCE.YYDESC_YTMM_AHFS_TEXT_AHFS_CODE,
        TARGET.YYLSTO_MARA = SOURCE.YYLSTO_MARA,
        TARGET.YYDESC_YTMM_LIST1_TEXT_LIST_1_CHEMICAL = SOURCE.YYDESC_YTMM_LIST1_TEXT_LIST_1_CHEMICAL,
        TARGET.YYFORM_MARA = SOURCE.YYFORM_MARA,
        TARGET.YYDESC_YTMM_FORM_TEXT_FORM = SOURCE.YYDESC_YTMM_FORM_TEXT_FORM,
        TARGET.YYCGCN_MARA = SOURCE.YYCGCN_MARA,
        TARGET.YYGENN_MARA = SOURCE.YYGENN_MARA,
        TARGET.YYSTRN_MARA = SOURCE.YYSTRN_MARA,
        TARGET.YYPCKG_MARA = SOURCE.YYPCKG_MARA,
        TARGET.YYDESC_YTMM_PACK_TEXT_PACKAGING_INDICATOR = SOURCE.YYDESC_YTMM_PACK_TEXT_PACKAGING_INDICATOR,
        TARGET.UNIT_DOSE_FLG = SOURCE.UNIT_DOSE_FLG,
        TARGET.YYSUPNR_MARA = SOURCE.YYSUPNR_MARA,
        TARGET.NAME1_LFA1 = SOURCE.NAME1_LFA1,
        TARGET.YYHAZSC_MARA = SOURCE.YYHAZSC_MARA,
        TARGET.YYDESC_YTMM_HAZMAT_TEXT_HAZMAT_STORAGE = SOURCE.YYDESC_YTMM_HAZMAT_TEXT_HAZMAT_STORAGE,
        TARGET.YYSPHD_MARA = SOURCE.YYSPHD_MARA,
        TARGET.YYDESC_YTMM_SPECL_TEXT_SPECIAL_HANDLING = SOURCE.YYDESC_YTMM_SPECL_TEXT_SPECIAL_HANDLING,
        TARGET.YYINJECT_MARA = SOURCE.YYINJECT_MARA,
        TARGET.YYFINE_MARA = SOURCE.YYFINE_MARA,
        TARGET.YYDESC_YTMM_FINE_TEXT_HAMACHER_FINE_CLASS = SOURCE.YYDESC_YTMM_FINE_TEXT_HAMACHER_FINE_CLASS,
        TARGET.YYFINER_MARA = SOURCE.YYFINER_MARA,
        TARGET.YYDESC_YTMM_FINER_TEXT_HAMACHER_FINER_CLASS = SOURCE.YYDESC_YTMM_FINER_TEXT_HAMACHER_FINER_CLASS,
        TARGET.YYFINEST_MARA = SOURCE.YYFINEST_MARA,
        TARGET.YYDESC_YTMM_FINEST_TEXT_HAMACHER_FINEST_CLASS = SOURCE.YYDESC_YTMM_FINEST_TEXT_HAMACHER_FINEST_CLASS,
        TARGET.YYFLASH_MARA = SOURCE.YYFLASH_MARA,
        TARGET.YYTNAM_MARA = SOURCE.YYTNAM_MARA,
        TARGET.YYCASN1_MARA = SOURCE.YYCASN1_MARA,
        TARGET.YYCASP1_MARA = SOURCE.YYCASP1_MARA,
        TARGET.YYCASN2_MARA = SOURCE.YYCASN2_MARA,
        TARGET.YYCASP2_MARA = SOURCE.YYCASP2_MARA,
        TARGET.YYCASN3_MARA = SOURCE.YYCASN3_MARA,
        TARGET.YYCASP3_MARA = SOURCE.YYCASP3_MARA,
        TARGET.YYCASN4_MARA = SOURCE.YYCASN4_MARA,
        TARGET.YYCASP4_MARA = SOURCE.YYCASP4_MARA,
        TARGET.YYCASN5_MARA = SOURCE.YYCASN5_MARA,
        TARGET.YYCASP5_MARA = SOURCE.YYCASP5_MARA,
        TARGET.YYCASN6_MARA = SOURCE.YYCASN6_MARA,
        TARGET.YYCASP6_MARA = SOURCE.YYCASP6_MARA,
        TARGET.YYDOTSP_MARA = SOURCE.YYDOTSP_MARA,
        TARGET.YYCMPTN_MARA = SOURCE.YYCMPTN_MARA,
        TARGET.YYHAZKEY_MARA = SOURCE.YYHAZKEY_MARA,
        TARGET.YYGCNSQN_MARA = SOURCE.YYGCNSQN_MARA,
        TARGET.YYBOXSTYLE_MARA = SOURCE.YYBOXSTYLE_MARA,
        TARGET.YYCHG3P_MARA = SOURCE.YYCHG3P_MARA,
        TARGET.YYLOGGR_MARA = SOURCE.YYLOGGR_MARA,
        TARGET.YYPACREF_MARA = SOURCE.YYPACREF_MARA,
        TARGET.YYDESC_YTMM_PACREF_TEXT_PACKAGE_REFERENCE = SOURCE.YYDESC_YTMM_PACREF_TEXT_PACKAGE_REFERENCE,
        TARGET.YYFORMFAM_MARA = SOURCE.YYFORMFAM_MARA,
        TARGET.YYNDCFDB_MARA = SOURCE.YYNDCFDB_MARA,
        TARGET.YYJCODEFDB_MARA = SOURCE.YYJCODEFDB_MARA,
        TARGET.YYJCODECAH_MARA = SOURCE.YYJCODECAH_MARA,
        TARGET.YYHCPCFDB_MARA = SOURCE.YYHCPCFDB_MARA,
        TARGET.YYHCPCCAH_MARA = SOURCE.YYHCPCCAH_MARA,
        TARGET.YYHCPCDESC_MARA = SOURCE.YYHCPCDESC_MARA,
        TARGET.YYHCPCDOSVAL_MARA = SOURCE.YYHCPCDOSVAL_MARA,
        TARGET.YYHCPCDOSUOM_MARA = SOURCE.YYHCPCDOSUOM_MARA,
        TARGET.YYPAYLIMFDB_MARA = SOURCE.YYPAYLIMFDB_MARA,
        TARGET.YYSTNGSP_MARA = SOURCE.YYSTNGSP_MARA,
        TARGET.YYSTNGUOMSP_MARA = SOURCE.YYSTNGUOMSP_MARA,
        TARGET.YYVOLSP_MARA = SOURCE.YYVOLSP_MARA,
        TARGET.YYVOLUOMSP_MARA = SOURCE.YYVOLUOMSP_MARA,
        TARGET.YYCONCNUM_MARA = SOURCE.YYCONCNUM_MARA,
        TARGET.YYCONCNUMUOM_MARA = SOURCE.YYCONCNUMUOM_MARA,
        TARGET.YYCONCDEN_MARA = SOURCE.YYCONCDEN_MARA,
        TARGET.YYCONCDENUOM_MARA = SOURCE.YYCONCDENUOM_MARA,
        TARGET.YYBILLVLSP_MARA = SOURCE.YYBILLVLSP_MARA,
        TARGET.YYBILLPKSP_MARA = SOURCE.YYBILLPKSP_MARA,
        TARGET.YYTAIDISP_MARA = SOURCE.YYTAIDISP_MARA,
        TARGET.YYTAIPRCH_MARA = SOURCE.YYTAIPRCH_MARA,
        TARGET.YYTAIUOM_MARA = SOURCE.YYTAIUOM_MARA,
        TARGET.YYSTKRQTY_MARA = SOURCE.YYSTKRQTY_MARA,
        TARGET.YYTRADNAME_MARA = SOURCE.YYTRADNAME_MARA,
        TARGET.YYCONKEY_MARA = SOURCE.YYCONKEY_MARA,
        TARGET.YYSRCSKEY_MARA = SOURCE.YYSRCSKEY_MARA,
        TARGET.YYGCNSQNC_MARA = SOURCE.YYGCNSQNC_MARA,
        TARGET.YYAHFSC_MARA = SOURCE.YYAHFSC_MARA,
        TARGET.CARD_AHFS_CDE_DESC_TXT = SOURCE.CARD_AHFS_CDE_DESC_TXT,
        TARGET.YYOBRAT_MARA = SOURCE.YYOBRAT_MARA,
        TARGET.FDB_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE = SOURCE.FDB_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE,
        TARGET.YYPKGKEY_MARA = SOURCE.YYPKGKEY_MARA,
        TARGET.YYPKGKEYC_MARA = SOURCE.YYPKGKEYC_MARA,
        TARGET.YYREMSP_MARA = SOURCE.YYREMSP_MARA,
        TARGET.YYDESC_YTMM_REMS_PROG_T_REMS_PROGRAM = SOURCE.YYDESC_YTMM_REMS_PROG_T_REMS_PROGRAM,
        TARGET.YYPKSZE_MARA = SOURCE.YYPKSZE_MARA,
        TARGET.CALC_PACK_QTY = SOURCE.CALC_PACK_QTY,
        TARGET.CALC_ACCUNET_QTY = SOURCE.CALC_ACCUNET_QTY,
        TARGET.YYMLTSRCF_MARA = SOURCE.YYMLTSRCF_MARA,
        TARGET.YYAWPM_MARA = SOURCE.YYAWPM_MARA,
        TARGET.YYUOMM_MARA = SOURCE.YYUOMM_MARA,
        TARGET.YYPRIU_MARA = SOURCE.YYPRIU_MARA,
        TARGET.YYDESC_YTMM_PRIU_T_PRICING_USAGE = SOURCE.YYDESC_YTMM_PRIU_T_PRICING_USAGE,
        TARGET.YYEDESC_MARA = SOURCE.YYEDESC_MARA,
        TARGET.YYOBRATC_MARA = SOURCE.YYOBRATC_MARA,
        TARGET.CARD_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE = SOURCE.CARD_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE,
        TARGET.YYREQG_MARA = SOURCE.YYREQG_MARA,
        TARGET.YYMANOVR_MARA = SOURCE.YYMANOVR_MARA,
        TARGET.YYOVRREA_MARA = SOURCE.YYOVRREA_MARA,
        TARGET.ZZTAXD_MARA = SOURCE.ZZTAXD_MARA,
        TARGET.DESCRIPTION_ZMT_PTS_TAX_DRVR_SALES_TAX_DRIVER = SOURCE.DESCRIPTION_ZMT_PTS_TAX_DRVR_SALES_TAX_DRIVER,
        TARGET.ZZCALPROP65_MARA = SOURCE.ZZCALPROP65_MARA,
        TARGET.YYOPIOID_MARA = SOURCE.YYOPIOID_MARA,
        TARGET.YYDEANAME_MARA = SOURCE.YYDEANAME_MARA,
        TARGET.YYNIOSH_MARA = SOURCE.YYNIOSH_MARA,
        TARGET.CALC_CARD_SBST_KEY_ID = SOURCE.CALC_CARD_SBST_KEY_ID,
        TARGET.CALC_CSE_QTY = SOURCE.CALC_CSE_QTY,
        TARGET.CALC_DOSE_QTY = SOURCE.CALC_DOSE_QTY,
        TARGET.CALC_CTN_QTY = SOURCE.CALC_CTN_QTY,
        TARGET.CALC_EACH_QTY = SOURCE.CALC_EACH_QTY,
        TARGET.CALC_RMBMT_UOM_CDE = SOURCE.CALC_RMBMT_UOM_CDE,
        TARGET.CALC_RMBMT_UOM_TXT = SOURCE.CALC_RMBMT_UOM_TXT,
        TARGET.CALC_RMBMT_UOM_QTY = SOURCE.CALC_RMBMT_UOM_QTY,
        TARGET.CALC_SHAPE = SOURCE.CALC_SHAPE,
        TARGET.CALC_COLOR = SOURCE.CALC_COLOR,
        TARGET.CALC_FLAVOR = SOURCE.CALC_FLAVOR,
        TARGET.DRUG_GROUP_ID_MEDISPAN_PROD = SOURCE.DRUG_GROUP_ID_MEDISPAN_PROD,
        TARGET.DRUG_GROUP_DESC_MEDISPAN = SOURCE.DRUG_GROUP_DESC_MEDISPAN,
        TARGET.DRUG_CLASS_ID_MEDISPAN_PROD = SOURCE.DRUG_CLASS_ID_MEDISPAN_PROD,
        TARGET.DRUG_CLASS_DESC_MEDISPAN = SOURCE.DRUG_CLASS_DESC_MEDISPAN,
        TARGET.DRUG_SUBCLS_ID_MEDISPAN_PROD = SOURCE.DRUG_SUBCLS_ID_MEDISPAN_PROD,
        TARGET.DRUG_SUBCLASS_DESC_MEDISPAN = SOURCE.DRUG_SUBCLASS_DESC_MEDISPAN,
        TARGET.GEN_PROD_ID_ARCH_MEDISPAN = SOURCE.GEN_PROD_ID_ARCH_MEDISPAN,
        TARGET.TEE_ID_ARCH_MEDISPAN = SOURCE.TEE_ID_ARCH_MEDISPAN,
        TARGET.YYBUNIT_MARA = SOURCE.YYBUNIT_MARA,
        TARGET.BILL_UNIT_MSEHL_T006A_UNIT_OF_MEASURE = SOURCE.BILL_UNIT_MSEHL_T006A_UNIT_OF_MEASURE,
        TARGET.BILL_UNIT_MSEHT_T006A_UNIT_OF_MEASURE = SOURCE.BILL_UNIT_MSEHT_T006A_UNIT_OF_MEASURE,
        TARGET.CALC_DSCSA_EXEMPT_ID = SOURCE.CALC_DSCSA_EXEMPT_ID,
        TARGET.CALC_FOOD_FLG = SOURCE.CALC_FOOD_FLG,
        TARGET.CALC_EXP_DTE_FLG = SOURCE.CALC_EXP_DTE_FLG,
        TARGET.CALC_VETERINARY_ID = SOURCE.CALC_VETERINARY_ID,
        TARGET.CALC_CONTAINS_BPA_ID = SOURCE.CALC_CONTAINS_BPA_ID,
        TARGET.CALC_CONTAINS_DEHP_ID = SOURCE.CALC_CONTAINS_DEHP_ID,
        TARGET.CALC_EPA_REGISTERED_ID = SOURCE.CALC_EPA_REGISTERED_ID,
        TARGET.CALC_ETO_STERILIZED_ID = SOURCE.CALC_ETO_STERILIZED_ID,
        TARGET.CALC_DIR_SRC_FLG = SOURCE.CALC_DIR_SRC_FLG,
        TARGET.CALC_SERIAL_FLG = SOURCE.CALC_SERIAL_FLG,
        TARGET.CALC_MED_FOOD_FLG = SOURCE.CALC_MED_FOOD_FLG,
        TARGET.CALC_KIT_WITH_DRUG_FLG = SOURCE.CALC_KIT_WITH_DRUG_FLG,
        TARGET.CALC_CA_PRPS_65_ID = SOURCE.CALC_CA_PRPS_65_ID,
        TARGET.CALC_PLASMA_ID = SOURCE.CALC_PLASMA_ID,
        TARGET.CALC_TRADE_PRTNR_TYPE_ID = SOURCE.CALC_TRADE_PRTNR_TYPE_ID,
        TARGET.CALC_NY_LAW_CDE_ID = SOURCE.CALC_NY_LAW_CDE_ID,
        TARGET.CALC_ADDL_MFG_INFO_FLG = SOURCE.CALC_ADDL_MFG_INFO_FLG,
        TARGET.CALC_BONUS_PACK_MTRL_NUM = SOURCE.CALC_BONUS_PACK_MTRL_NUM,
        TARGET.CALC_PURCH_BLCK_REVIEW_NEED_FLG = SOURCE.CALC_PURCH_BLCK_REVIEW_NEED_FLG,
        TARGET.CALC_STICKERS_ONLY_FLG = SOURCE.CALC_STICKERS_ONLY_FLG,
        TARGET.CALC_BRAND_GEN_EXCP_ID = SOURCE.CALC_BRAND_GEN_EXCP_ID,
        TARGET.CALC_DEMAND_FORECASTING_ALRG_FLG = SOURCE.CALC_DEMAND_FORECASTING_ALRG_FLG,
        TARGET.CALC_DEMAND_FORECASTING_COUGH_COLD_FLG = SOURCE.CALC_DEMAND_FORECASTING_COUGH_COLD_FLG,
        TARGET.CALC_FLU_ID = SOURCE.CALC_FLU_ID,
        TARGET.CALC_MEDSURG_FLG = SOURCE.CALC_MEDSURG_FLG,
        TARGET.CALC_MEDSURG_ID = SOURCE.CALC_MEDSURG_ID,
        TARGET.CALC_COVID_ID = SOURCE.CALC_COVID_ID,
        TARGET.CALC_KINRAY_ITEM_FLG = SOURCE.CALC_KINRAY_ITEM_FLG,
        TARGET.CALC_PUERTO_RICO_ITEM_ID = SOURCE.CALC_PUERTO_RICO_ITEM_ID,
        TARGET.CALC_BIOLOGIC_BLA_TYPE_ID = SOURCE.CALC_BIOLOGIC_BLA_TYPE_ID,
        TARGET.CALC_IB_SPECIAL_PROCESS_FLG = SOURCE.CALC_IB_SPECIAL_PROCESS_FLG,
        TARGET.CALC_CNSMR_HLTH_PLNGRM_FLG = SOURCE.CALC_CNSMR_HLTH_PLNGRM_FLG,
        TARGET.CALC_VACCINE_PORTAL_ID = SOURCE.CALC_VACCINE_PORTAL_ID,
        TARGET.COMPOSITE_KEY = SOURCE.COMPOSITE_KEY,
        TARGET.ROW_ADD_STP = SOURCE.ROW_ADD_STP,
        TARGET.ROW_ADD_USER_ID = SOURCE.ROW_ADD_USER_ID,
        TARGET.ROW_UPDATE_STP = SOURCE.ROW_UPDATE_STP,
        TARGET.ROW_UPDATE_USER_ID = SOURCE.ROW_UPDATE_USER_ID,
        TARGET.CALC_BASE_UOM_UPC_ID = SOURCE.CALC_BASE_UOM_UPC_ID,
        TARGET.CALC_D0_START_STP = SOURCE.CALC_D0_START_STP,
        TARGET.ENTERED_DTE_ITEM_MAST = SOURCE.ENTERED_DTE_ITEM_MAST,
        TARGET.CALC_MED_INTGRT_DSPNS_FLG = SOURCE.CALC_MED_INTGRT_DSPNS_FLG,
        TARGET.CALC_ECOM_PROD_CTLG_FLG = SOURCE.CALC_ECOM_PROD_CTLG_FLG
    WHEN NOT MATCHED THEN
      INSERT VALUES
      (
        SOURCE.MATNR_MARA,
        SOURCE.VRSN_START_DTE,
        SOURCE.VRSN_END_DTE,
        SOURCE.CURR_VRSN_FLG,
        SOURCE.MAKTX_MAKT,
        SOURCE.CALC_ERSDA_MARA,
        SOURCE.ERNAM_MARA,
        SOURCE.LAST_CHG_DTE,
        SOURCE.AENAM_MARA,
        SOURCE.LVORM_MARA,
        SOURCE.MTART_MARA,
        SOURCE.MTBEZ_T134T_MATERIAL_TYPE,
        SOURCE.MBRSH_MARA,
        SOURCE.MBBEZ_T137T_INDUSTRY_SECTOR,
        SOURCE.MATKL_MARA,
        SOURCE.WGBEZ_T023T_MATERIAL_GROUP,
        SOURCE.MEINS_MARA,
        SOURCE.BASE_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.BSTME_MARA,
        SOURCE.PURCH_ORDER_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.GROES_MARA,
        SOURCE.BRGEW_MARA,
        SOURCE.NTGEW_MARA,
        SOURCE.GEWEI_MARA,
        SOURCE.WEIGHT_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.VOLUM_MARA,
        SOURCE.VOLEH_MARA,
        SOURCE.VOLUME_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.RAUBE_MARA,
        SOURCE.RBTXT_T142T_STORAGE_CONDITION,
        SOURCE.TEMPB_MARA,
        SOURCE.TBTXT_T143T_TEMPERATURE_CONDITION,
        SOURCE.CALC_REFRIG_FLG,
        SOURCE.DISST_MARA,
        SOURCE.TRAGR_MARA,
        SOURCE.VTEXT_TTGRT_TRANSPORTATION_GROUP,
        SOURCE.SPART_MARA,
        SOURCE.CALC_NDC_ID,
        SOURCE.CALC_NDC_CTGRY_ID,
        SOURCE.CALC_EACH_UPC_ID,
        SOURCE.CALC_EACH_UPC_CTGRY_ID,
        SOURCE.CALC_CSE_UPC_ID,
        SOURCE.CALC_CSE_UPC_CTGRY_ID,
        SOURCE.CALC_CTN_UPC_ID,
        SOURCE.CALC_CTN_UPC_CTGRY_ID,
        SOURCE.CALC_EACH_EAN_ID,
        SOURCE.CALC_CSE_EAN_ID,
        SOURCE.CALC_CTN_EAN_ID,
        SOURCE.CALC_EACH_GTIN_ID,
        SOURCE.CALC_CSE_GTIN_ID,
        SOURCE.CALC_CTN_GTIN_ID,
        SOURCE.LAENG_MARA,
        SOURCE.BREIT_MARA,
        SOURCE.HOEHE_MARA,
        SOURCE.MEABM_MARA,
        SOURCE.DIM_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.PRDHA_MARA,
        SOURCE.VTEXT_T179T_PRODUCT_HIERARCHY,
        SOURCE.VABME_MARA,
        SOURCE.VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT,
        SOURCE.VHART_MARA,
        SOURCE.VTEXT_TVTYT_PACKAGING_MATERIAL_TYPE,
        SOURCE.MAGRV_MARA,
        SOURCE.BEZEI_TVEGRT_MTRL_GRP_PACKAGING_MATERIALS,
        SOURCE.EXTWG_MARA,
        SOURCE.MSTAE_MARA,
        SOURCE.MTSTB_T141T_CROSS_PLANT_MATERIAL_STATUS,
        SOURCE.MSTAV_MARA,
        SOURCE.VMSTB_TVMST_CROSS_DISTRIB_CHAIN_MTRL_STATUS,
        SOURCE.MSTDE_MARA,
        SOURCE.MSTDV_MARA,
        SOURCE.MHDRZ_MARA,
        SOURCE.KZUMW_MARA,
        SOURCE.KOSCH_MARA,
        SOURCE.VTEXT_T190ST_PRODUCT_ALLOCATION,
        SOURCE.MFRPN_MARA,
        SOURCE.MFRNR_MARA,
        SOURCE.PROFL_MARA,
        SOURCE.PROFLD_TDG42_DANGEROUS_GOODS_INDICATOR,
        SOURCE.MTPOS_MARA_MARA,
        SOURCE.BEZEI_TPTMT_ITEM_ITEM_CATEGORY_GROUP,
        SOURCE.SLED_BBD_MARA,
        SOURCE.EXP_DTE_IND_DESC_TXT,
        SOURCE.CALC_GEN_OTC_FLG,
        SOURCE.CALC_RX_FLG,
        SOURCE.CALC_DEA_SCHED_ID,
        SOURCE.BEZEI_T606V_LEGAL_CONTROL,
        SOURCE.YYBASE_MARA,
        SOURCE.YYDESC_YTMM_DEA_TEXT_DEA_BASE_CODE,
        SOURCE.YYSUBB_MARA,
        SOURCE.YYDESC_YTMM_SUBBASETEXT_DEA_SUB_BASE_CODE,
        SOURCE.YYLDESC_MARA,
        SOURCE.YYAHFS_MARA,
        SOURCE.YYDESC_YTMM_AHFS_TEXT_AHFS_CODE,
        SOURCE.YYLSTO_MARA,
        SOURCE.YYDESC_YTMM_LIST1_TEXT_LIST_1_CHEMICAL,
        SOURCE.YYFORM_MARA,
        SOURCE.YYDESC_YTMM_FORM_TEXT_FORM,
        SOURCE.YYCGCN_MARA,
        SOURCE.YYGENN_MARA,
        SOURCE.YYSTRN_MARA,
        SOURCE.YYPCKG_MARA,
        SOURCE.YYDESC_YTMM_PACK_TEXT_PACKAGING_INDICATOR,
        SOURCE.UNIT_DOSE_FLG,
        SOURCE.YYSUPNR_MARA,
        SOURCE.NAME1_LFA1,
        SOURCE.YYHAZSC_MARA,
        SOURCE.YYDESC_YTMM_HAZMAT_TEXT_HAZMAT_STORAGE,
        SOURCE.YYSPHD_MARA,
        SOURCE.YYDESC_YTMM_SPECL_TEXT_SPECIAL_HANDLING,
        SOURCE.YYINJECT_MARA,
        SOURCE.YYFINE_MARA,
        SOURCE.YYDESC_YTMM_FINE_TEXT_HAMACHER_FINE_CLASS,
        SOURCE.YYFINER_MARA,
        SOURCE.YYDESC_YTMM_FINER_TEXT_HAMACHER_FINER_CLASS,
        SOURCE.YYFINEST_MARA,
        SOURCE.YYDESC_YTMM_FINEST_TEXT_HAMACHER_FINEST_CLASS,
        SOURCE.YYFLASH_MARA,
        SOURCE.YYTNAM_MARA,
        SOURCE.YYCASN1_MARA,
        SOURCE.YYCASP1_MARA,
        SOURCE.YYCASN2_MARA,
        SOURCE.YYCASP2_MARA,
        SOURCE.YYCASN3_MARA,
        SOURCE.YYCASP3_MARA,
        SOURCE.YYCASN4_MARA,
        SOURCE.YYCASP4_MARA,
        SOURCE.YYCASN5_MARA,
        SOURCE.YYCASP5_MARA,
        SOURCE.YYCASN6_MARA,
        SOURCE.YYCASP6_MARA,
        SOURCE.YYDOTSP_MARA,
        SOURCE.YYCMPTN_MARA,
        SOURCE.YYHAZKEY_MARA,
        SOURCE.YYGCNSQN_MARA,
        SOURCE.YYBOXSTYLE_MARA,
        SOURCE.YYCHG3P_MARA,
        SOURCE.YYLOGGR_MARA,
        SOURCE.YYPACREF_MARA,
        SOURCE.YYDESC_YTMM_PACREF_TEXT_PACKAGE_REFERENCE,
        SOURCE.YYFORMFAM_MARA,
        SOURCE.YYNDCFDB_MARA,
        SOURCE.YYJCODEFDB_MARA,
        SOURCE.YYJCODECAH_MARA,
        SOURCE.YYHCPCFDB_MARA,
        SOURCE.YYHCPCCAH_MARA,
        SOURCE.YYHCPCDESC_MARA,
        SOURCE.YYHCPCDOSVAL_MARA,
        SOURCE.YYHCPCDOSUOM_MARA,
        SOURCE.YYPAYLIMFDB_MARA,
        SOURCE.YYSTNGSP_MARA,
        SOURCE.YYSTNGUOMSP_MARA,
        SOURCE.YYVOLSP_MARA,
        SOURCE.YYVOLUOMSP_MARA,
        SOURCE.YYCONCNUM_MARA,
        SOURCE.YYCONCNUMUOM_MARA,
        SOURCE.YYCONCDEN_MARA,
        SOURCE.YYCONCDENUOM_MARA,
        SOURCE.YYBILLVLSP_MARA,
        SOURCE.YYBILLPKSP_MARA,
        SOURCE.YYTAIDISP_MARA,
        SOURCE.YYTAIPRCH_MARA,
        SOURCE.YYTAIUOM_MARA,
        SOURCE.YYSTKRQTY_MARA,
        SOURCE.YYTRADNAME_MARA,
        SOURCE.YYCONKEY_MARA,
        SOURCE.YYSRCSKEY_MARA,
        SOURCE.YYGCNSQNC_MARA,
        SOURCE.YYAHFSC_MARA,
        SOURCE.CARD_AHFS_CDE_DESC_TXT,
        SOURCE.YYOBRAT_MARA,
        SOURCE.FDB_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE,
        SOURCE.YYPKGKEY_MARA,
        SOURCE.YYPKGKEYC_MARA,
        SOURCE.YYREMSP_MARA,
        SOURCE.YYDESC_YTMM_REMS_PROG_T_REMS_PROGRAM,
        SOURCE.YYPKSZE_MARA,
        SOURCE.CALC_PACK_QTY,
        SOURCE.CALC_ACCUNET_QTY,
        SOURCE.YYMLTSRCF_MARA,
        SOURCE.YYAWPM_MARA,
        SOURCE.YYUOMM_MARA,
        SOURCE.YYPRIU_MARA,
        SOURCE.YYDESC_YTMM_PRIU_T_PRICING_USAGE,
        SOURCE.YYEDESC_MARA,
        SOURCE.YYOBRATC_MARA,
        SOURCE.CARD_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE,
        SOURCE.YYREQG_MARA,
        SOURCE.YYMANOVR_MARA,
        SOURCE.YYOVRREA_MARA,
        SOURCE.ZZTAXD_MARA,
        SOURCE.DESCRIPTION_ZMT_PTS_TAX_DRVR_SALES_TAX_DRIVER,
        SOURCE.ZZCALPROP65_MARA,
        SOURCE.YYOPIOID_MARA,
        SOURCE.YYDEANAME_MARA,
        SOURCE.YYNIOSH_MARA,
        SOURCE.CALC_CARD_SBST_KEY_ID,
        SOURCE.CALC_CSE_QTY,
        SOURCE.CALC_DOSE_QTY,
        SOURCE.CALC_CTN_QTY,
        SOURCE.CALC_EACH_QTY,
        SOURCE.CALC_RMBMT_UOM_CDE,
        SOURCE.CALC_RMBMT_UOM_TXT,
        SOURCE.CALC_RMBMT_UOM_QTY,
        SOURCE.CALC_SHAPE,
        SOURCE.CALC_COLOR,
        SOURCE.CALC_FLAVOR,
        SOURCE.DRUG_GROUP_ID_MEDISPAN_PROD,
        SOURCE.DRUG_GROUP_DESC_MEDISPAN,
        SOURCE.DRUG_CLASS_ID_MEDISPAN_PROD,
        SOURCE.DRUG_CLASS_DESC_MEDISPAN,
        SOURCE.DRUG_SUBCLS_ID_MEDISPAN_PROD,
        SOURCE.DRUG_SUBCLASS_DESC_MEDISPAN,
        SOURCE.GEN_PROD_ID_ARCH_MEDISPAN,
        SOURCE.TEE_ID_ARCH_MEDISPAN,
        SOURCE.YYBUNIT_MARA,
        SOURCE.BILL_UNIT_MSEHL_T006A_UNIT_OF_MEASURE,
        SOURCE.BILL_UNIT_MSEHT_T006A_UNIT_OF_MEASURE,
        SOURCE.CALC_DSCSA_EXEMPT_ID,
        SOURCE.CALC_FOOD_FLG,
        SOURCE.CALC_EXP_DTE_FLG,
        SOURCE.CALC_VETERINARY_ID,
        SOURCE.CALC_CONTAINS_BPA_ID,
        SOURCE.CALC_CONTAINS_DEHP_ID,
        SOURCE.CALC_EPA_REGISTERED_ID,
        SOURCE.CALC_ETO_STERILIZED_ID,
        SOURCE.CALC_DIR_SRC_FLG,
        SOURCE.CALC_SERIAL_FLG,
        SOURCE.CALC_MED_FOOD_FLG,
        SOURCE.CALC_KIT_WITH_DRUG_FLG,
        SOURCE.CALC_CA_PRPS_65_ID,
        SOURCE.CALC_PLASMA_ID,
        SOURCE.CALC_TRADE_PRTNR_TYPE_ID,
        SOURCE.CALC_NY_LAW_CDE_ID,
        SOURCE.CALC_ADDL_MFG_INFO_FLG,
        SOURCE.CALC_BONUS_PACK_MTRL_NUM,
        SOURCE.CALC_PURCH_BLCK_REVIEW_NEED_FLG,
        SOURCE.CALC_STICKERS_ONLY_FLG,
        SOURCE.CALC_BRAND_GEN_EXCP_ID,
        SOURCE.CALC_DEMAND_FORECASTING_ALRG_FLG,
        SOURCE.CALC_DEMAND_FORECASTING_COUGH_COLD_FLG,
        SOURCE.CALC_FLU_ID,
        SOURCE.CALC_MEDSURG_FLG,
        SOURCE.CALC_MEDSURG_ID,
        SOURCE.CALC_COVID_ID,
        SOURCE.CALC_KINRAY_ITEM_FLG,
        SOURCE.CALC_PUERTO_RICO_ITEM_ID,
        SOURCE.CALC_BIOLOGIC_BLA_TYPE_ID,
        SOURCE.CALC_IB_SPECIAL_PROCESS_FLG,
        SOURCE.CALC_CNSMR_HLTH_PLNGRM_FLG,
        SOURCE.CALC_VACCINE_PORTAL_ID,
        SOURCE.COMPOSITE_KEY,
        SOURCE.ROW_ADD_STP,
        SOURCE.ROW_ADD_USER_ID,
        SOURCE.ROW_UPDATE_STP,
        SOURCE.ROW_UPDATE_USER_ID,
        SOURCE.CALC_BASE_UOM_UPC_ID,
        SOURCE.CALC_D0_START_STP,
        SOURCE.ENTERED_DTE_ITEM_MAST,
        SOURCE.CALC_MED_INTGRT_DSPNS_FLG,
        SOURCE.CALC_ECOM_PROD_CTLG_FLG
      );
    END;
