CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_MASTER_CV`
(
  NDC_UPC_HRI_ID STRING OPTIONS(description=" NDC-UPC-HRI "),
  CHG_FLAG STRING OPTIONS(description=" Change Flag "),
  SEQ_ID STRING OPTIONS(description=" Sequence Code  "),
  LABELE_CDE STRING OPTIONS(description=" Labeler Code "),
  GEN_TYPE_ID STRING OPTIONS(description=" Generic ID Type Code "),
  GEN_ID_NUM STRING OPTIONS(description=" Generic ID Number  "),
  DEA_CLASS_ID STRING OPTIONS(description=" DEA Class Code  "),
  AHFS_THER_CLASS_CDE STRING OPTIONS(description=" AHFS Therapeutic Class Codes "),
  ITEM_STAT_FLG STRING OPTIONS(description=" Item Status Flag  "),
  LOCAL_SYSTEM_FLG STRING OPTIONS(description=" Local/Systemic Flag "),
  TEE_ID STRING OPTIONS(description=" TEE ID "),
  FMT_ID STRING OPTIONS(description=" Formatted ID Number "),
  RX_OTC_ID STRING OPTIONS(description=" RX-OTC Indicator Code "),
  THIRD_PARTY_RSTRCT_ID STRING OPTIONS(description=" Third-Party Restriction Code "),
  MAINT_DRUG_ID STRING OPTIONS(description=" Maintenance Drug Code "),
  DSPNS_UNIT_ID STRING OPTIONS(description=" Dispensing Unit Code "),
  UNIT_DOSE_PACK_ID STRING OPTIONS(description=" Unit-Dose/Unit-of-Use Package Code "),
  ROUTE_OF_ADMIN_ID STRING OPTIONS(description=" Route of Administration "),
  FORM_TYPE_ID STRING OPTIONS(description=" Form Type Code  "),
  DLR_RANK_ID STRING OPTIONS(description=" Dollar Rank Code "),
  RX_RANK_ID STRING OPTIONS(description=" Rx Rank Code  "),
  NUM_SYSTEM_CHAR_ID STRING OPTIONS(description=" Number System Character "),
  SCNDRY_FMT_ID STRING OPTIONS(description=" Secondary Format ID Code "),
  SCNDRY_ID STRING OPTIONS(description=" Secondary ID Number "),
  MULTI_SRC_ID STRING OPTIONS(description=" Multi-Source Code  "),
  BRAND_NAM_ID STRING OPTIONS(description=" Brand Name Code  "),
  INTRNL_EXTRNL_ID STRING OPTIONS(description=" Internal-External Code "),
  SINGLE_CMBNTN_ID STRING OPTIONS(description=" Single-Combination Code "),
  STRG_CNDTN_ID STRING OPTIONS(description=" Storage Condition Code "),
  LMT_STBLTY_ID STRING OPTIONS(description=" Limited Stability Code "),
  OLD_NDC_UPC_HRI_ID STRING OPTIONS(description=" Old NDC-UPC-HRI "),
  OLD_FMT_ID STRING OPTIONS(description=" Old Formatted ID Number "),
  OLD_EFFECT_DTE STRING OPTIONS(description=" Old Eff Date "),
  NEW_NDC_UPC_HRI_ID STRING OPTIONS(description=" New NDC-UPC-HRI "),
  NEW_FMT_ID STRING OPTIONS(description=" New Formatted ID Number "),
  NEW_EFFECT_DTE STRING OPTIONS(description=" New Eff Date "),
  PROD_NAM STRING OPTIONS(description=" Product Name  "),
  PROD_EXTN_NAM STRING OPTIONS(description=" Product Name Extension "),
  ALRG_PTRN_CDE STRING OPTIONS(description=" Allergy Pattern Code  "),
  PPG_ID STRING OPTIONS(description=" PPG Indicator Code  "),
  HFPG_ID STRING OPTIONS(description=" HFPG Indicator Code "),
  LABELE_TYPE_ID STRING OPTIONS(description=" Labeler Type Code  "),
  PRICE_SPREAD_ID STRING OPTIONS(description=" Pricing Spread Code  "),
  MFG_NAM STRING OPTIONS(description="Manufacturer’s (Labeler) Name  "),
  MFG_ABBR_NAM STRING OPTIONS(description=" Manufacturer’s Abbreviated Name  "),
  PROD_ABBR_DESC STRING OPTIONS(description=" Product Description Abbreviation "),
  DRUG_NAM_ID STRING OPTIONS(description=" Drug Name Code "),
  GEN_PROD_PACK_ID STRING OPTIONS(description=" Generic Product Packaging Code "),
  GEN_PROD_ID STRING OPTIONS(description=" Generic Product Identifier  "),
  GPI_GEN_NAM STRING OPTIONS(description=" GPI Generic Name "),
  GEN_PROD_PACK_SIZE_QTY STRING OPTIONS(description=" Generic Package Size "),
  GEN_PROD_PACK_SIZE_UOM_ID STRING OPTIONS(description=" Generic Package Size Unit of Measure"),
  GEN_PROD_PACK_QTY STRING OPTIONS(description=" Generic Package Quantity "),
  GEN_PROD_UNIT_DOSE_USE_PACK_ID STRING OPTIONS(description=" Generic Unit Dose/Unit-of-Use Package Code "),
  GEN_PROD_PACK_DESC_ID STRING OPTIONS(description=" Generic Package Description Code "),
  DOSAGE_FORM_ID STRING OPTIONS(description=" Dosage Form "),
  PACK_SIZE_QTY STRING OPTIONS(description=" Package Size "),
  PACK_SIZE_UOM_ID STRING OPTIONS(description=" Package Size Unit of Measure "),
  PACK_QTY STRING OPTIONS(description=" Package Quantity "),
  REPACK_ID STRING OPTIONS(description=" Repackage Code "),
  TOTAL_PACK_QTY STRING OPTIONS(description=" Total Package Quantity "),
  DESI_ID STRING OPTIONS(description=" DESI Code "),
  PACK_DESC STRING OPTIONS(description=" Package Description "),
  NEXT_SMALL_NDC_SUFFIX_ID STRING OPTIONS(description=" Next-Smaller NDC Suffix Number "),
  NEXT_LARGE_NDC_SUFFIX_ID STRING OPTIONS(description=" Next-Larger NDC Suffix Number "),
  INNER_PACK_ID STRING OPTIONS(description=" Innerpack Code "),
  CLINIC_PACK_ID STRING OPTIONS(description=" Clinic Pack Code "),
  METRIC_STRGTH_ID STRING OPTIONS(description=" Metric Strength "),
  STRGTH_UOM_ID STRING OPTIONS(description=" Strength Unit of Measure "),
  LMT_DIST_ID STRING OPTIONS(description=" Limited Distribution Code  "),
  EXT_AHFS_THER_CLASS_ID STRING OPTIONS(description=" Extended AHFS Therapeutic Class Code "),
  INACT_DTE STRING OPTIONS(description=" Inactive Date "),
  CMS_FUL_PRICE_AMT STRING OPTIONS(description=" CMS FUL Price  "),
  CMS_FUL_EFFECT_DTE STRING OPTIONS(description=" CMS FUL Price Effective Date "),
  FIRST_OLD_CMS_FUL_PRICE_AMT STRING OPTIONS(description=" 1st Oldest CMS FUL Price "),
  FIRST_OLD_CMS_FUL_EFFECT_DTE STRING OPTIONS(description=" 1st Oldest CMS FUL Price Effective Date "),
  SCND_OLD_CMS_FUL_LMT_PRICE_AMT STRING OPTIONS(description=" 2nd Oldest CMS FUL Price "),
  SCND_OLD_CMS_FUL_EFFECT_DTE STRING OPTIONS(description=" 2nd Oldest CMS FUL Price Effective Date "),
  WAC_PACK_PRICE_AMT STRING OPTIONS(description=" WAC Package Price "),
  WAC_UNIT_PRICE_AMT STRING OPTIONS(description=" WAC Unit Price "),
  WAC_EFFECT_DTE STRING OPTIONS(description=" Effective Date "),
  OLD_WAC_PACK_PRICE_AMT STRING OPTIONS(description=" Older WAC Package Price "),
  OLD_WAC_UNIT_PRICE_AMT STRING OPTIONS(description=" Older WAC Unit Price "),
  OLD_WAC_EFFECT_DTE STRING OPTIONS(description=" Older WAC Effective Date "),
  AWP_ID STRING OPTIONS(description=" AWP Indicator Code "),
  AWP_PACK_PRICE_AMT STRING OPTIONS(description=" AWP Package Price "),
  AWP_UNIT_PRICE_AMT STRING OPTIONS(description=" AWP Unit Price "),
  AWP_EFFECT_DTE STRING OPTIONS(description=" Effective Date "),
  OLD_AWP_PACK_PRICE_AMT STRING OPTIONS(description=" Older AWP Package Price "),
  OLD_AWP_UNIT_PRICE_AMT STRING OPTIONS(description=" Older AWP Unit Price "),
  OLD_AWP_EFFECT_DTE STRING OPTIONS(description=" Older AWP Effective Date "),
  DP_PACK_PRICE_AMT STRING OPTIONS(description=" DP Package Price "),
  DP_UNIT_PRICE_AMT STRING OPTIONS(description=" DP Unit Price "),
  DP_EFFECT_DTE STRING OPTIONS(description=" Effective Date "),
  OLD_DP_PACK_PRICE_AMT STRING OPTIONS(description=" Older DP Package Price "),
  OLD_DP_UNIT_PRICE_AMT STRING OPTIONS(description=" Older DP Unit Price "),
  OLD_DP_EFFECT_DTE STRING OPTIONS(description=" Older DP Effective Date "),
  GPPC_UNIT_PRICE_AMT STRING OPTIONS(description=" GPPC Unit Price "),
  GPPC_EFFECT_DTE STRING OPTIONS(description=" GPPC Eff Date "),
  DRUG_GROUP_CDE STRING OPTIONS(description=" Drug Group Cde "),
  DRUG_GROUP_DESCRIPTION STRING OPTIONS(description=" Drug Group Description "),
  DRUG_CLASS_CDE STRING OPTIONS(description=" Drug Class Cde "),
  DRUG_CLASS_DESCRIPTION STRING OPTIONS(description=" Drug Class Description "),
  DRUG_SUB_CLASS_CDE STRING OPTIONS(description=" Drug Sub Class Cde "),
  DRUG_SUB_CLASS_DESCRIPTION STRING OPTIONS(description=" Drug Sub Class Description "),
  DRUG_CDE STRING OPTIONS(description=" Drug Cde "),
  DRUG_DESCRIPTION STRING OPTIONS(description=" Drug Description "),
  DEA_CLASS_DESCRIPTION STRING OPTIONS(description=" DEA Class Description "),
  TEE_DESCRIPTION STRING OPTIONS(description=" TEE Description "),
  RX_OTC_DESCRIPTION STRING OPTIONS(description=" RX OTC Description "),
  THIRD_PARTY_ID_DESCRIPTION STRING OPTIONS(description=" Third Party ID Description "),
  UNIT_DOSE_PACK_ID_DESCRIPTION STRING OPTIONS(description=" Unit Dose Pack ID Description "),
  ROUTE_OF_ADMIN_ID_DESCRIPTION STRING OPTIONS(description=" Route of Admin Id Description "),
  MULTI_SOURCE_ID_DESCRIPTION STRING OPTIONS(description=" Multi Source ID Description "),
  BRAND_NAME_ID_DESCRIPTION STRING OPTIONS(description=" Brand Name Id Description "),
  DOSAGE_FORM_ID_DESCRIPTION STRING OPTIONS(description=" Dosage Form Id Description "),
  PACK_SIZE_UOM_ID_DESCRIPTION STRING OPTIONS(description=" Pack Size UOM Id Description "),
  DESI_ID_DESCRIPTION STRING OPTIONS(description=" Desi Id Description "),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC_UPC_HRI_ID;