CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MEDISPAN_MASTER_CV`(
  NDC_UPC_HRI_ID OPTIONS(description=" NDC-UPC-HRI "),
  CHG_FLAG OPTIONS(description=" Change Flag "),
  SEQ_ID OPTIONS(description=" Sequence Code  "),
  LABELE_CDE OPTIONS(description=" Labeler Code "),
  GEN_TYPE_ID OPTIONS(description=" Generic ID Type Code "),
  GEN_ID_NUM OPTIONS(description=" Generic ID Number  "),
  DEA_CLASS_ID OPTIONS(description=" DEA Class Code  "),
  AHFS_THER_CLASS_CDE OPTIONS(description=" AHFS Therapeutic Class Codes "),
  ITEM_STAT_FLG OPTIONS(description=" Item Status Flag  "),
  LOCAL_SYSTEM_FLG OPTIONS(description=" Local/Systemic Flag "),
  TEE_ID OPTIONS(description=" TEE ID "),
  FMT_ID OPTIONS(description=" Formatted ID Number "),
  RX_OTC_ID OPTIONS(description=" RX-OTC Indicator Code "),
  THIRD_PARTY_RSTRCT_ID OPTIONS(description=" Third-Party Restriction Code "),
  MAINT_DRUG_ID OPTIONS(description=" Maintenance Drug Code "),
  DSPNS_UNIT_ID OPTIONS(description=" Dispensing Unit Code "),
  UNIT_DOSE_PACK_ID OPTIONS(description=" Unit-Dose/Unit-of-Use Package Code "),
  ROUTE_OF_ADMIN_ID OPTIONS(description=" Route of Administration "),
  FORM_TYPE_ID OPTIONS(description=" Form Type Code  "),
  DLR_RANK_ID OPTIONS(description=" Dollar Rank Code "),
  RX_RANK_ID OPTIONS(description=" Rx Rank Code  "),
  NUM_SYSTEM_CHAR_ID OPTIONS(description=" Number System Character "),
  SCNDRY_FMT_ID OPTIONS(description=" Secondary Format ID Code "),
  SCNDRY_ID OPTIONS(description=" Secondary ID Number "),
  MULTI_SRC_ID OPTIONS(description=" Multi-Source Code  "),
  BRAND_NAM_ID OPTIONS(description=" Brand Name Code  "),
  INTRNL_EXTRNL_ID OPTIONS(description=" Internal-External Code "),
  SINGLE_CMBNTN_ID OPTIONS(description=" Single-Combination Code "),
  STRG_CNDTN_ID OPTIONS(description=" Storage Condition Code "),
  LMT_STBLTY_ID OPTIONS(description=" Limited Stability Code "),
  OLD_NDC_UPC_HRI_ID OPTIONS(description=" Old NDC-UPC-HRI "),
  OLD_FMT_ID OPTIONS(description=" Old Formatted ID Number "),
  OLD_EFFECT_DTE OPTIONS(description=" Old Eff Date "),
  NEW_NDC_UPC_HRI_ID OPTIONS(description=" New NDC-UPC-HRI "),
  NEW_FMT_ID OPTIONS(description=" New Formatted ID Number "),
  NEW_EFFECT_DTE OPTIONS(description=" New Eff Date "),
  PROD_NAM OPTIONS(description=" Product Name  "),
  PROD_EXTN_NAM OPTIONS(description=" Product Name Extension "),
  ALRG_PTRN_CDE OPTIONS(description=" Allergy Pattern Code  "),
  PPG_ID OPTIONS(description=" PPG Indicator Code  "),
  HFPG_ID OPTIONS(description=" HFPG Indicator Code "),
  LABELE_TYPE_ID OPTIONS(description=" Labeler Type Code  "),
  PRICE_SPREAD_ID OPTIONS(description=" Pricing Spread Code  "),
  MFG_NAM OPTIONS(description="Manufacturer’s (Labeler) Name  "),
  MFG_ABBR_NAM OPTIONS(description=" Manufacturer’s Abbreviated Name  "),
  PROD_ABBR_DESC OPTIONS(description=" Product Description Abbreviation "),
  DRUG_NAM_ID OPTIONS(description=" Drug Name Code "),
  GEN_PROD_PACK_ID OPTIONS(description=" Generic Product Packaging Code "),
  GEN_PROD_ID OPTIONS(description=" Generic Product Identifier  "),
  GPI_GEN_NAM OPTIONS(description=" GPI Generic Name "),
  GEN_PROD_PACK_SIZE_QTY OPTIONS(description=" Generic Package Size "),
  GEN_PROD_PACK_SIZE_UOM_ID OPTIONS(description=" Generic Package Size Unit of Measure"),
  GEN_PROD_PACK_QTY OPTIONS(description=" Generic Package Quantity "),
  GEN_PROD_UNIT_DOSE_USE_PACK_ID OPTIONS(description=" Generic Unit Dose/Unit-of-Use Package Code "),
  GEN_PROD_PACK_DESC_ID OPTIONS(description=" Generic Package Description Code "),
  DOSAGE_FORM_ID OPTIONS(description=" Dosage Form "),
  PACK_SIZE_QTY OPTIONS(description=" Package Size "),
  PACK_SIZE_UOM_ID OPTIONS(description=" Package Size Unit of Measure "),
  PACK_QTY OPTIONS(description=" Package Quantity "),
  REPACK_ID OPTIONS(description=" Repackage Code "),
  TOTAL_PACK_QTY OPTIONS(description=" Total Package Quantity "),
  DESI_ID OPTIONS(description=" DESI Code "),
  PACK_DESC OPTIONS(description=" Package Description "),
  NEXT_SMALL_NDC_SUFFIX_ID OPTIONS(description=" Next-Smaller NDC Suffix Number "),
  NEXT_LARGE_NDC_SUFFIX_ID OPTIONS(description=" Next-Larger NDC Suffix Number "),
  INNER_PACK_ID OPTIONS(description=" Innerpack Code "),
  CLINIC_PACK_ID OPTIONS(description=" Clinic Pack Code "),
  METRIC_STRGTH_ID OPTIONS(description=" Metric Strength "),
  STRGTH_UOM_ID OPTIONS(description=" Strength Unit of Measure "),
  LMT_DIST_ID OPTIONS(description=" Limited Distribution Code  "),
  EXT_AHFS_THER_CLASS_ID OPTIONS(description=" Extended AHFS Therapeutic Class Code "),
  INACT_DTE OPTIONS(description=" Inactive Date "),
  CMS_FUL_PRICE_AMT OPTIONS(description=" CMS FUL Price  "),
  CMS_FUL_EFFECT_DTE OPTIONS(description=" CMS FUL Price Effective Date "),
  FIRST_OLD_CMS_FUL_PRICE_AMT OPTIONS(description=" 1st Oldest CMS FUL Price "),
  FIRST_OLD_CMS_FUL_EFFECT_DTE OPTIONS(description=" 1st Oldest CMS FUL Price Effective Date "),
  SCND_OLD_CMS_FUL_LMT_PRICE_AMT OPTIONS(description=" 2nd Oldest CMS FUL Price "),
  SCND_OLD_CMS_FUL_EFFECT_DTE OPTIONS(description=" 2nd Oldest CMS FUL Price Effective Date "),
  WAC_PACK_PRICE_AMT OPTIONS(description=" WAC Package Price "),
  WAC_UNIT_PRICE_AMT OPTIONS(description=" WAC Unit Price "),
  WAC_EFFECT_DTE OPTIONS(description=" Effective Date "),
  OLD_WAC_PACK_PRICE_AMT OPTIONS(description=" Older WAC Package Price "),
  OLD_WAC_UNIT_PRICE_AMT OPTIONS(description=" Older WAC Unit Price "),
  OLD_WAC_EFFECT_DTE OPTIONS(description=" Older WAC Effective Date "),
  AWP_ID OPTIONS(description=" AWP Indicator Code "),
  AWP_PACK_PRICE_AMT OPTIONS(description=" AWP Package Price "),
  AWP_UNIT_PRICE_AMT OPTIONS(description=" AWP Unit Price "),
  AWP_EFFECT_DTE OPTIONS(description=" Effective Date "),
  OLD_AWP_PACK_PRICE_AMT OPTIONS(description=" Older AWP Package Price "),
  OLD_AWP_UNIT_PRICE_AMT OPTIONS(description=" Older AWP Unit Price "),
  OLD_AWP_EFFECT_DTE OPTIONS(description=" Older AWP Effective Date "),
  DP_PACK_PRICE_AMT OPTIONS(description=" DP Package Price "),
  DP_UNIT_PRICE_AMT OPTIONS(description=" DP Unit Price "),
  DP_EFFECT_DTE OPTIONS(description=" Effective Date "),
  OLD_DP_PACK_PRICE_AMT OPTIONS(description=" Older DP Package Price "),
  OLD_DP_UNIT_PRICE_AMT OPTIONS(description=" Older DP Unit Price "),
  OLD_DP_EFFECT_DTE OPTIONS(description=" Older DP Effective Date "),
  GPPC_UNIT_PRICE_AMT OPTIONS(description=" GPPC Unit Price "),
  GPPC_EFFECT_DTE OPTIONS(description=" GPPC Eff Date "),
  DRUG_GROUP_CDE OPTIONS(description=" Drug Group Cde "),
  DRUG_GROUP_DESCRIPTION OPTIONS(description=" Drug Group Description "),
  DRUG_CLASS_CDE OPTIONS(description=" Drug Class Cde "),
  DRUG_CLASS_DESCRIPTION OPTIONS(description=" Drug Class Description "),
  DRUG_SUB_CLASS_CDE OPTIONS(description=" Drug Sub Class Cde "),
  DRUG_SUB_CLASS_DESCRIPTION OPTIONS(description=" Drug Sub Class Description "),
  DRUG_CDE OPTIONS(description=" Drug Cde "),
  DRUG_DESCRIPTION OPTIONS(description=" Drug Description "),
  DEA_CLASS_DESCRIPTION OPTIONS(description=" DEA Class Description "),
  TEE_DESCRIPTION OPTIONS(description=" TEE Description "),
  RX_OTC_DESCRIPTION OPTIONS(description=" RX OTC Description "),
  THIRD_PARTY_ID_DESCRIPTION OPTIONS(description=" Third Party ID Description "),
  UNIT_DOSE_PACK_ID_DESCRIPTION OPTIONS(description=" Unit Dose Pack ID Description "),
  ROUTE_OF_ADMIN_ID_DESCRIPTION OPTIONS(description=" Route of Admin Id Description "),
  MULTI_SOURCE_ID_DESCRIPTION OPTIONS(description=" Multi Source ID Description "),
  BRAND_NAME_ID_DESCRIPTION OPTIONS(description=" Brand Name Id Description "),
  DOSAGE_FORM_ID_DESCRIPTION OPTIONS(description=" Dosage Form Id Description "),
  PACK_SIZE_UOM_ID_DESCRIPTION OPTIONS(description=" Pack Size UOM Id Description "),
  DESI_ID_DESCRIPTION OPTIONS(description=" Desi Id Description "),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
NDC_UPC_HRI_ID
,CHG_FLAG
,SEQ_ID
,LABELE_CDE
,GEN_TYPE_ID
,GEN_ID_NUM
,DEA_CLASS_ID
,AHFS_THER_CLASS_CDE
,ITEM_STAT_FLG
,LOCAL_SYSTEM_FLG
,TEE_ID
,FMT_ID
,RX_OTC_ID
,THIRD_PARTY_RSTRCT_ID
,MAINT_DRUG_ID
,DSPNS_UNIT_ID
,UNIT_DOSE_PACK_ID
,ROUTE_OF_ADMIN_ID
,FORM_TYPE_ID
,DLR_RANK_ID
,RX_RANK_ID
,NUM_SYSTEM_CHAR_ID
,SCNDRY_FMT_ID
,SCNDRY_ID
,MULTI_SRC_ID
,BRAND_NAM_ID
,INTRNL_EXTRNL_ID
,SINGLE_CMBNTN_ID
,STRG_CNDTN_ID
,LMT_STBLTY_ID
,OLD_NDC_UPC_HRI_ID
,OLD_FMT_ID
,OLD_EFFECT_DTE
,NEW_NDC_UPC_HRI_ID
,NEW_FMT_ID
,NEW_EFFECT_DTE
,PROD_NAM
,PROD_EXTN_NAM
,ALRG_PTRN_CDE
,PPG_ID
,HFPG_ID
,LABELE_TYPE_ID
,PRICE_SPREAD_ID
,MFG_NAM
,MFG_ABBR_NAM
,PROD_ABBR_DESC
,DRUG_NAM_ID
,GEN_PROD_PACK_ID
,GEN_PROD_ID
,GPI_GEN_NAM
,GEN_PROD_PACK_SIZE_QTY
,GEN_PROD_PACK_SIZE_UOM_ID
,GEN_PROD_PACK_QTY
,GEN_PROD_UNIT_DOSE_USE_PACK_ID
,GEN_PROD_PACK_DESC_ID
,DOSAGE_FORM_ID
,PACK_SIZE_QTY
,PACK_SIZE_UOM_ID
,PACK_QTY
,REPACK_ID
,TOTAL_PACK_QTY
,DESI_ID
,PACK_DESC
,NEXT_SMALL_NDC_SUFFIX_ID
,NEXT_LARGE_NDC_SUFFIX_ID
,INNER_PACK_ID
,CLINIC_PACK_ID
,METRIC_STRGTH_ID
,STRGTH_UOM_ID
,LMT_DIST_ID
,EXT_AHFS_THER_CLASS_ID
,INACT_DTE
,CMS_FUL_PRICE_AMT
,CMS_FUL_EFFECT_DTE
,FIRST_OLD_CMS_FUL_PRICE_AMT
,FIRST_OLD_CMS_FUL_EFFECT_DTE
,SCND_OLD_CMS_FUL_LMT_PRICE_AMT
,SCND_OLD_CMS_FUL_EFFECT_DTE
,WAC_PACK_PRICE_AMT
,WAC_UNIT_PRICE_AMT
,WAC_EFFECT_DTE
,OLD_WAC_PACK_PRICE_AMT
,OLD_WAC_UNIT_PRICE_AMT
,OLD_WAC_EFFECT_DTE
,AWP_ID
,AWP_PACK_PRICE_AMT
,AWP_UNIT_PRICE_AMT
,AWP_EFFECT_DTE
,OLD_AWP_PACK_PRICE_AMT
,OLD_AWP_UNIT_PRICE_AMT
,OLD_AWP_EFFECT_DTE
,DP_PACK_PRICE_AMT
,DP_UNIT_PRICE_AMT
,DP_EFFECT_DTE
,OLD_DP_PACK_PRICE_AMT
,OLD_DP_UNIT_PRICE_AMT
,OLD_DP_EFFECT_DTE
,GPPC_UNIT_PRICE_AMT
,GPPC_EFFECT_DTE
,DRUG_GROUP_CDE
,DRUG_GROUP_DESCRIPTION
,DRUG_CLASS_CDE
,DRUG_CLASS_DESCRIPTION
,DRUG_SUB_CLASS_CDE
,DRUG_SUB_CLASS_DESCRIPTION
,DRUG_CDE
,DRUG_DESCRIPTION
,DEA_CLASS_DESCRIPTION
,TEE_DESCRIPTION
,RX_OTC_DESCRIPTION
,THIRD_PARTY_ID_DESCRIPTION
,UNIT_DOSE_PACK_ID_DESCRIPTION
,ROUTE_OF_ADMIN_ID_DESCRIPTION
,MULTI_SOURCE_ID_DESCRIPTION
,BRAND_NAME_ID_DESCRIPTION
,DOSAGE_FORM_ID_DESCRIPTION
,PACK_SIZE_UOM_ID_DESCRIPTION
,DESI_ID_DESCRIPTION
,ROW_ADD_STP
,ROW_ADD_USER_ID
,ROW_UPDATE_STP
,ROW_UPDATE_USER_ID

FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_MASTER_CV`;