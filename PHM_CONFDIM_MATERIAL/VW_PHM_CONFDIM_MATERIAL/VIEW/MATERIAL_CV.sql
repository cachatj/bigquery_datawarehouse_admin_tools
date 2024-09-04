CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_CV`(
  MTRL_NUM OPTIONS(description="Material Number"),
  VRSN_START_DTE OPTIONS(description="Version Start Date"),
  VRSN_END_DTE OPTIONS(description="Version End Date"),
  CURR_VRSN_FLG OPTIONS(description="Current Version Flag"),
  MTRL_DESC_TXT OPTIONS(description="Material Description Text"),
  CREATE_ON_DTE OPTIONS(description="Created On Date"),
  CREATE_BY_USER_ID OPTIONS(description="Created by User ID"),
  LAST_CHG_DTE OPTIONS(description="Last Change Date"),
  CHG_BY_USER_ID OPTIONS(description="Changed by User ID"),
  CALC_DEL_IND_FLG OPTIONS(description="Calculated Deletion Indicator Flag"),
  MTRL_TYPE_LKP_ID OPTIONS(description="Material Type Lookup ID"),
  MTRL_TYPE_DESC_TXT OPTIONS(description="Material Type Description Text"),
  INDST_SCTR_LKP_ID OPTIONS(description="Industry Sector Lookup ID"),
  INDST_SCTR_DESC_TXT OPTIONS(description="Industry Sector Description Text"),
  MTRL_GROUP_LKP_ID OPTIONS(description="Material Group Lookup ID"),
  MTRL_GROUP_DESC_TXT OPTIONS(description="Material Group Description Text"),
  BASE_UOM_LKP_ID OPTIONS(description="Base UOM Lookup ID"),
  BASE_UOM_DESC_TXT OPTIONS(description="Base UOM Description Text"),
  PURCH_ORDER_UOM_LKP_ID OPTIONS(description="Purchase Order UOM Lookup ID"),
  PURCH_ORDER_UOM_DESC_TXT OPTIONS(description="Purchase Order UOM Description Text"),
  SIZE_DIM_TXT OPTIONS(description="Size dimensions Text"),
  GROS_WEIGHT_NUM OPTIONS(description="Gross Weight Number"),
  NET_WEIGHT_NUM OPTIONS(description="Net Weight Number"),
  WEIGHT_UOM_LKP_ID OPTIONS(description="Weight UOM Lookup ID"),
  WEIGHT_UOM_DESC_TXT OPTIONS(description="Weight UOM Description Text"),
  VOLUME_NUM OPTIONS(description="Volume Number"),
  VOLUME_UOM_LKP_ID OPTIONS(description="Volume UOM Lookup ID"),
  VOLUME_UOM_DESC_TXT OPTIONS(description="Volume UOM Description Text"),
  STRG_CNDTN_CDE OPTIONS(description="Storage conditions Code"),
  STRG_CNDTN_DESC_TXT OPTIONS(description="Storage conditions Description Text"),
  TMPTR_CNDTN_CDE OPTIONS(description="Temperature condition Code"),
  TMPTR_CNDTN_DESC_TXT OPTIONS(description="Temperature condition Description Text"),
  CALC_REFRIG_FLG OPTIONS(description="Calculated Refrigeration Flag"),
  LOW_LEVEL_CDE_TXT OPTIONS(description="Low Level Code Text"),
  TRNSPT_GROUP_CDE OPTIONS(description="Transportation Group Code"),
  TRNSPT_GROUP_DESC_TXT OPTIONS(description="Transportation Group Description Text"),
  DIV_ID OPTIONS(description="Division ID"),
  CALC_NDC_ID OPTIONS(description="Calculated NDC ID"),
  CALC_NDC_CTGRY_ID OPTIONS(description="Calculated NDC Category ID"),
  CALC_EACH_UPC_ID OPTIONS(description="Calculated Each UPC ID"),
  CALC_EACH_UPC_CTGRY_ID OPTIONS(description="Calculated Each UPC Category ID"),
  CALC_CSE_UPC_ID OPTIONS(description="Calculated Case UPC ID"),
  CALC_CSE_UPC_CTGRY_ID OPTIONS(description="Calculated Case UPC Category ID"),
  CALC_CTN_UPC_ID OPTIONS(description="Calculated Carton UPC ID"),
  CALC_CTN_UPC_CTGRY_ID OPTIONS(description="Calculated Carton UPC Category ID"),
  CALC_EACH_EAN_ID OPTIONS(description="Calculated Each EAN ID"),
  CALC_CSE_EAN_ID OPTIONS(description="Calculated Case EAN ID"),
  CALC_CTN_EAN_ID OPTIONS(description="Calculated Carton EAN ID"),
  CALC_EACH_GTIN_ID OPTIONS(description="Calculated Each GTIN ID"),
  CALC_CSE_GTIN_ID OPTIONS(description="Calculated Case GTIN ID"),
  CALC_CTN_GTIN_ID OPTIONS(description="Calculated Carton GTIN ID"),
  LGTH_NUM OPTIONS(description="Length Number"),
  WIDTH_NUM OPTIONS(description="Width Number"),
  HEIGHT_NUM OPTIONS(description="Height Number"),
  DIM_UOM_LKP_ID OPTIONS(description="Dimension UOM Lookup ID"),
  DIM_UOM_DESC_TXT OPTIONS(description="Dimension UOM Description Text"),
  PROD_HRCHY_LKP_ID OPTIONS(description="Product hierarchy Lookup ID"),
  PROD_HRCHY_DESC_TXT OPTIONS(description="Product Hierarchy Description Text"),
  VRBL_PURCH_ORDER_UNIT_ACT_LKP_ID OPTIONS(description="Variable Purchase Order Unit Activity Lookup ID"),
  VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT OPTIONS(description="Variable Purchase Order Unit Activity Description Text"),
  PMTL_TYPE_LKP_ID OPTIONS(description="Packaging material type Lookup ID"),
  PMTL_TYPE_DESC_TXT OPTIONS(description="Packaging Material Type Description Text"),
  MTRL_GROUP_PACK_MTRL_LKP_ID OPTIONS(description="Material Group Packaging Materials Lookup ID"),
  MTRL_GROUP_PACK_MTRL_DESC_TXT OPTIONS(description="Material Group Packaging Materials Description Text"),
  EXT_MTRL_GROUP_ID OPTIONS(description="Ext Material Group ID"),
  XPLNT_MTRL_STAT_LKP_ID OPTIONS(description="Cross Plant Material Status Lookup ID"),
  XPLNT_MTRL_STAT_DESC_TXT OPTIONS(description="Cross Plant Material Status Description Text"),
  XDC_MTRL_STAT_LKP_ID OPTIONS(description="Cross Distribution Chain Material Status Lookup ID"),
  XDC_MTRL_STAT_DESC_TXT OPTIONS(description="Cross Distribution Chain Material Status Description Text"),
  XPLNT_MTRL_STAT_VALID_FROM_DTE OPTIONS(description="Cross Plant Material Status Valid from Date"),
  XDC_STAT_VALID_FROM_DTE OPTIONS(description="Cross Distribution Chain Status Valid from Date"),
  MIN_RMN_SHELF_LIFE_NUM OPTIONS(description="Minimum Remaining Shelf Life Number"),
  ENVR_RLVNT_FLG OPTIONS(description="Environmentally Relevant Flag"),
  PROD_ALLCT_LKP_ID OPTIONS(description="Product Allocation Lookup ID"),
  PROD_ALLCT_DESC_TXT OPTIONS(description="Product Allocation Description Text"),
  MFG_PART_ID OPTIONS(description="Manufacturer Part ID"),
  MFG_NUM OPTIONS(description="Manufacturer Number"),
  DGNR_GOOD_IND_PRFL_LKP_ID OPTIONS(description="Dangerous Good indicator profile Lookup ID"),
  DGNR_GOODS_IND_PRFL_DESC_TXT OPTIONS(description="Dangerous Goods indicator profile Description Text"),
  ITEM_CTGRY_GROUP_LKP_ID OPTIONS(description="Item Category Group Lookup ID"),
  ITEM_CTGRY_GROUP_DESC_TXT OPTIONS(description="Item Category Group Description Text"),
  EXP_DTE_IND_ID OPTIONS(description="Expiration Date Indicator ID"),
  EXP_DTE_IND_DESC_TXT OPTIONS(description="Expiration Date Indicator Description Text"),
  CALC_GEN_OTC_FLG OPTIONS(description="Calculated Generic OTC Flag"),
  CALC_RX_FLG OPTIONS(description="Calculated RX Flag"),
  CALC_DEA_SCHED_ID OPTIONS(description="Calculated DEA Schedule ID"),
  DEA_SCHED_NUM_DESC_TXT OPTIONS(description="DEA Schedule Number Description Text"),
  DEA_BASE_CDE_LKP_ID OPTIONS(description="DEA Base Code Lookup ID"),
  DEA_BASE_CDE_DESC_TXT OPTIONS(description="DEA Base Code Description Text"),
  DEA_SUB_BASE_CDE_LKP_ID OPTIONS(description="DEA Sub Base Code Lookup ID"),
  DEA_SUB_BASE_CDE_DESC_TXT OPTIONS(description="DEA Sub Base Code Description Text"),
  LONG_MTRL_DESC_TXT OPTIONS(description="Long Material Description Text"),
  AHFS_LKP_ID OPTIONS(description="AHFS Lookup ID"),
  AHFS_DESC_TXT OPTIONS(description="AHFS Description Text"),
  LIST_1_CHEM_LKP_ID OPTIONS(description="List 1 Chemical Lookup ID"),
  LIST_1_CHEM_DESC_TXT OPTIONS(description="List 1 Chemical Description Text"),
  FORM_LKP_ID OPTIONS(description="Form Lookup ID"),
  FORM_DESC_TXT OPTIONS(description="Form Description Text"),
  GEN_CDE_NUM OPTIONS(description="Generic Code Number"),
  GEN_NAM_TXT OPTIONS(description="Generic Name Text"),
  STRGTH_TXT OPTIONS(description="Strength Text"),
  PACK_IND_LKP_ID OPTIONS(description="Packaging Indicator Lookup ID"),
  PACK_IND_DESC_TXT OPTIONS(description="Packaging Indicator Description Text"),
  UNIT_DOSE_FLG OPTIONS(description="Unit Dose Flag"),
  SUPPLIER_NUM OPTIONS(description="Supplier Number"),
  SUPPLIER_NAM OPTIONS(description="Supplier Name"),
  HAZMAT_STRG_LKP_ID OPTIONS(description="Hazmat Storage Lookup ID"),
  HAZMAT_STRG_DESC_TXT OPTIONS(description="Hazmat Storage Description Text"),
  SPECIAL_HNDL_LKP_ID OPTIONS(description="Special Handling Lookup ID"),
  SPECIAL_HNDL_DESC_TXT OPTIONS(description="Special Handling Description Text"),
  INJCT_FLG OPTIONS(description="InjecVIEW Flag"),
  HAM_FINE_CLASS_LKP_ID OPTIONS(description="Hamacher Fine Class Lookup ID"),
  HAM_FINE_CLASS_DESC_TXT OPTIONS(description="Hamacher Fine Class Description Text"),
  HAM_FINER_CLASS_CDE_LKP_ID OPTIONS(description="Hamacher Finer Class Code Lookup ID"),
  HAM_FINER_CLASS_DESC_TXT OPTIONS(description="Hamacher Finer Class Description Text"),
  HAM_FINEST_CLASS_LKP_ID OPTIONS(description="Hamacher Finest Class Lookup ID"),
  HAM_FINEST_CLASS_DESC_TXT OPTIONS(description="Hamacher Finest Class Description Text"),
  FLSH_POINT_TXT OPTIONS(description="Flash Point Text"),
  TECH_NAM_TXT OPTIONS(description="Technical Name Text"),
  CAS_1_NUM OPTIONS(description="CAS 1 Number"),
  CAS_1_PCT OPTIONS(description="CAS 1 Percentage"),
  CAS_2_NUM OPTIONS(description="CAS 2 Number"),
  CAS_2_PCT OPTIONS(description="CAS 2 Percentage"),
  CAS_3_NUM OPTIONS(description="CAS 3 Number"),
  CAS_3_PCT OPTIONS(description="CAS 3 Percentage"),
  CAS_4_NUM OPTIONS(description="CAS 4 Number"),
  CAS_4_PCT OPTIONS(description="CAS 4 Percentage"),
  CAS_5_NUM OPTIONS(description="CAS 5 Number"),
  CAS_5_PCT OPTIONS(description="CAS 5 Percentage"),
  CAS_6_NUM OPTIONS(description="CAS 6 Number"),
  CAS_6_PCT OPTIONS(description="CAS 6 Percentage"),
  DOT_SPECIAL_PRMT_TXT OPTIONS(description="DOT Special Permit Text"),
  CMPR_TO_TXT OPTIONS(description="Compare To Text"),
  HAZMAT_KEY_NUM OPTIONS(description="Hazmat Key Number"),
  GCN_SEQ_NUM OPTIONS(description="GCN Sequence Number"),
  BOX_STYL_ID OPTIONS(description="Box Style ID"),
  THIRD_PARTY_DATA_CHG_NUM OPTIONS(description="Third Party Data Change Num"),
  MTRL_PTWY_ID OPTIONS(description="Material Putaway ID"),
  PACK_RFRNC_GUIDE_LKP_ID OPTIONS(description="Package Reference Guide Lookup ID"),
  PACK_RFRNC_GUIDE_DESC_TXT OPTIONS(description="Package Reference Guide Description Text"),
  FORM_FMLY_ID OPTIONS(description="Form Family ID"),
  FDB_NDC_CDE OPTIONS(description="FDB NDC Code"),
  FDB_J_CDE OPTIONS(description="FDB J Code"),
  CAH_J_CDE OPTIONS(description="CAH J Code"),
  FDB_HCPC_CDE OPTIONS(description="FDB HCPC Code"),
  CAH_HCPC_CDE OPTIONS(description="CAH HCPC Code"),
  HCPC_DESC_TXT OPTIONS(description="HCPC Description Text"),
  HCPC_DOSAGE_VALUE_NUM OPTIONS(description="HCPC Dosage Value Number"),
  HCPC_DOSAGE_UOM_ID OPTIONS(description="HCPC Dosage UOM ID"),
  FDB_PYMT_LMT_AMT OPTIONS(description="FDB Payment Limit Amount"),
  SPECIALTY_STRGTH_VALUE_NUM OPTIONS(description="Specialty Strength Value Number"),
  SPECIALTY_STRGTH_UOM_ID OPTIONS(description="Specialty Strength UOM ID"),
  SPECIALTY_VOLUME_VALUE_NUM OPTIONS(description="Specialty Volume Value Number"),
  SPECIALTY_VOLUME_UOM_ID OPTIONS(description="Specialty Volume UOM ID"),
  SPECIALTY_CNCTRT_NMRTR_VALUE_NUM OPTIONS(description="Specialty Concentration Numerator Value Number"),
  SPECIALTY_CNCTRT_NMRTR_UOM_ID OPTIONS(description="Specialty Concentration Numerator UOM ID"),
  SPECIALTY_CNCTRT_DMNTR_VALUE_NUM OPTIONS(description="Specialty Concentration Denominator Value Number"),
  SPECIALTY_CNCTRT_DMNTR_UOM_ID OPTIONS(description="Specialty Concentration Denominator UOM ID"),
  SPECIALTY_BILL_UNIT_VIAL_NUM OPTIONS(description="Specialty Bill Units Vial Number"),
  SPECIALTY_BILL_UNIT_PACK_NUM OPTIONS(description="Specialty Bill Units Package Number"),
  SPECIALTY_TOTAL_ACTIVE_INGRED_DISP_NUM OPTIONS(description="Specialty Total Active Ingredient Disp Number"),
  SPECIALTY_TOTAL_ACTIVE_INGRED_PURCH_NUM OPTIONS(description="Specialty Total Active Ingredient Purchase Number"),
  SPECIALTY_TOTAL_ACTIVE_INGRED_UOM_ID OPTIONS(description="Specialty Total Active Ingredient UOM ID"),
  STICKER_QTY OPTIONS(description="Sticker Quantity"),
  TRADE_NAM OPTIONS(description="Trade Name"),
  CNSMR_KEY_NUM OPTIONS(description="Consumer Key Number"),
  SRC_SUB_KEY_ID OPTIONS(description="Sourcing Sub Key ID"),
  CARD_GCN_SEQ_NUM OPTIONS(description="Cardinal GCN Sequence Number"),
  CARD_AHFS_CDE OPTIONS(description="Cardinal AHFS Code"),
  CARD_AHFS_CDE_DESC_TXT OPTIONS(description="Cardinal AHFS Code Description Text"),
  FDB_OBC_LKP_ID OPTIONS(description="FDB OBC Lookup ID"),
  FDB_OBC_DESC_TXT OPTIONS(description="FDB OBC Description Text"),
  PACK_KEY_ID OPTIONS(description="Packaging Key ID"),
  CAH_PACK_KEY_ID OPTIONS(description="CAH Packaging Key ID"),
  REMS_PROG_LKP_ID OPTIONS(description="REMS Program Lookup ID"),
  REMS_PROG_DESC_TXT OPTIONS(description="REMS Program Description Text"),
  PACK_SIZE_NUM OPTIONS(description="Pack Size Number"),
  CALC_PACK_QTY OPTIONS(description="Calculated Pack Quantity"),
  CALC_ACCUNET_QTY OPTIONS(description="Calculated Accunet Quantity"),
  FDB_MULTI_SRC_IND_ID OPTIONS(description="FDB Multi Source Indicator ID"),
  MDSP_AWP_AMT OPTIONS(description="Medispan AWP Amount"),
  MDSP_UOM_ID OPTIONS(description="Medispan UOM ID"),
  PRICE_USAGE_LKP_ID OPTIONS(description="Pricing Usage Lookup ID"),
  PRICE_USAGE_DESC_TXT OPTIONS(description="Pricing Usage Description Text"),
  EXPANDED_MTRL_DESC_TXT OPTIONS(description="Expanded Material Description Text"),
  CARD_OBC_LKP_ID OPTIONS(description="Cardinal OBC Lookup ID"),
  CARD_OBC_DESC_TXT OPTIONS(description="Cardinal OBC Description Text"),
  REQ_GROUP_CDE_ID OPTIONS(description="Request Group Code ID"),
  MANUAL_OVRD_FLG OPTIONS(description="Manual Override Flag"),
  OVRD_REASON_TXT OPTIONS(description="Override Reason Text"),
  SALE_TAX_DRVR_LKP_ID OPTIONS(description="Sales Tax Driver Lookup ID"),
  SALE_TAX_DRVR_DESC_TXT OPTIONS(description="Sales Tax Driver Description Text"),
  CA_PRPS_65_ID OPTIONS(description="California Proposition 65 ID"),
  OPIOID_FLG OPTIONS(description="Opioid Flag"),
  DEA_NAM_TXT OPTIONS(description="DEA Name Text"),
  NIOSH_KEY_ID OPTIONS(description="NIOSH Key ID"),
  CALC_CARD_SBST_KEY_ID OPTIONS(description="Calculated Cardinal Substitution Key ID"),
  CALC_CSE_QTY OPTIONS(description="Calculated Case Quantity"),
  CALC_DOSE_QTY OPTIONS(description="Calculated Dose Quantity"),
  CALC_CTN_QTY OPTIONS(description="Calculated Carton Quantity"),
  CALC_EACH_QTY OPTIONS(description="Calculated Each Quantity"),
  CALC_RMBMT_UOM_CDE OPTIONS(description="Calculated Reimbursement UOM Code"),
  CALC_RMBMT_UOM_TXT OPTIONS(description="Calculated Reimbursement UOM Text"),
  CALC_RMBMT_UOM_QTY OPTIONS(description="Calculated Reimbursement UOM Quantity"),
  SHAPE_TXT OPTIONS(description="Shape Text"),
  COLOR_TXT OPTIONS(description="Color Text"),
  FLAVOR_TXT OPTIONS(description="Flavor Text"),
  MDSP_DRUG_GROUP_ID OPTIONS(description="Medispan Drug Group ID"),
  MDSP_DRUG_GROUP_DESC_TXT OPTIONS(description="Medispan Drug Group Description Text"),
  MDSP_DRUG_CLASS_ID OPTIONS(description="Medispan Drug Class ID"),
  MDSP_DRUG_CLASS_DESC_TXT OPTIONS(description="Medispan Drug Class Description Text"),
  MDSP_DRUG_SUBCLS_ID OPTIONS(description="Medispan Drug Subclass ID"),
  MDSP_DRUG_SUBCLS_DESC_TXT OPTIONS(description="Medispan Drug Subclass Description Text"),
  MDSP_GEN_PROD_ID OPTIONS(description="Medispan Generic Product ID"),
  MDSP_THER_EQV_ID OPTIONS(description="Medispan Therapeutic Equivalent ID"),
  BILL_UOM_ID OPTIONS(description="Billing UOM ID"),
  BILL_UOM_DESC_TXT OPTIONS(description="Billing UOM Description"),
  BILL_UOM_SHRT_DESC_TXT OPTIONS(description="Billing UOM Short Description"),
  CALC_DSCSA_EXEMPT_ID OPTIONS(description="Calculated DSCSA Exempt ID"),
  CALC_FOOD_FLG OPTIONS(description="Calculated Food Flag"),
  CALC_EXP_DTE_FLG OPTIONS(description="Calculated Expiration Date Flag"),
  CALC_VETERINARY_ID OPTIONS(description="Calculated Veterinary ID"),
  CALC_CONTAINS_BPA_ID OPTIONS(description="Calculated Contains BPA ID"),
  CALC_CONTAINS_DEHP_ID OPTIONS(description="Calculated Contains DEHP ID"),
  CALC_EPA_REGISTERED_ID OPTIONS(description="Calculated EPA Registered ID"),
  CALC_ETO_STERILIZED_ID OPTIONS(description="Calculated ETO Sterilized ID"),
  CALC_DIR_SRC_FLG OPTIONS(description="Calculated Direct Source Flag"),
  CALC_SERIAL_FLG OPTIONS(description="Calculated Serialized Flag"),
  CALC_MED_FOOD_FLG OPTIONS(description="Calculated Medical Food Flag"),
  CALC_KIT_WITH_DRUG_FLG OPTIONS(description="Calculated Kit With Drug Flag"),
  CALC_CA_PRPS_65_ID OPTIONS(description="Calculated California Proposition 65 ID"),
  CALC_PLASMA_ID OPTIONS(description="Calculated Plasma ID"),
  CALC_TRADE_PRTNR_TYPE_ID OPTIONS(description="Calculated Trading Partner Type ID"),
  CALC_NY_LAW_CDE_ID OPTIONS(description="Calculated NY Law Code ID"),
  CALC_ADDL_MFG_INFO_FLG OPTIONS(description="Calculated Additional Manufacturer Info Flag"),
  CALC_BONUS_PACK_MTRL_NUM OPTIONS(description="Calculated Bonus Pack Material Number"),
  CALC_PURCH_BLCK_REVIEW_NEED_FLG OPTIONS(description="Calculated Purchase Block Review Needed Flag"),
  CALC_STICKERS_ONLY_FLG OPTIONS(description="Calculated Stickers Only Flag"),
  CALC_BRAND_GEN_EXCP_ID OPTIONS(description="Calculated Brand Generic Exception ID"),
  CALC_DEMAND_FORECASTING_ALRG_FLG OPTIONS(description="Calculated Demand Forecasting Allergy Flag"),
  CALC_DEMAND_FORECASTING_COUGH_COLD_FLG OPTIONS(description="Calculated Demand Forecasting Cough Cold Flag"),
  CALC_FLU_ID OPTIONS(description="Calculated Flu ID"),
  CALC_MEDSURG_FLG OPTIONS(description="Calculated Medsurg Flag"),
  CALC_MEDSURG_ID OPTIONS(description="Calculated Medsurg ID"),
  CALC_COVID_ID OPTIONS(description="Calculated COVID ID"),
  CALC_KINRAY_ITEM_FLG OPTIONS(description="Calculated Kinray Item Flag"),
  CALC_PUERTO_RICO_ITEM_ID OPTIONS(description="Calculated Puerto Rico Item ID"),
  CALC_BIOLOGIC_BLA_TYPE_ID OPTIONS(description="Calculated Biologic BLA Type ID"),
  CALC_IB_SPECIAL_PROCESS_FLG OPTIONS(description="Calculated IB Special Processing Flag"),
  CALC_CNSMR_HLTH_PLNGRM_FLG OPTIONS(description="Calculated Consumer Health Planogram Flag"),
  CALC_VACCINE_PORTAL_ID OPTIONS(description="Calculated Vaccine Portal ID"),
  COMPOSITE_KEY OPTIONS(description="Composite key to identify records in initial load"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID"),
  CALC_BASE_UOM_UPC_ID OPTIONS(description="Base UoM UPC ID"),
  CALC_D0_START_STP OPTIONS(description="Indicates When The Record Was Added By Attinuity, Internal use only"),
  ENTERED_DTE OPTIONS(description="MIF Create Date"),
  CALC_MED_INTGRT_DSPNS_FLG OPTIONS(description="Calculated Medically Integrated Dispensing Flag"),
  CALC_ECOM_PROD_CTLG_FLG OPTIONS(description="Calculated E-Commerce Product Catalog Flag")
)
AS SELECT
 MATNR_MARA     AS     MTRL_NUM
,VRSN_START_DTE     AS     VRSN_START_DTE
,VRSN_END_DTE     AS     VRSN_END_DTE
,CURR_VRSN_FLG     AS     CURR_VRSN_FLG
,MAKTX_MAKT     AS     MTRL_DESC_TXT
,CALC_ERSDA_MARA     AS     CREATE_ON_DTE
,ERNAM_MARA     AS     CREATE_BY_USER_ID
,LAST_CHG_DTE     AS     LAST_CHG_DTE
,AENAM_MARA     AS     CHG_BY_USER_ID
,LVORM_MARA     AS     CALC_DEL_IND_FLG
,MTART_MARA     AS     MTRL_TYPE_LKP_ID
,MTBEZ_T134T_MATERIAL_TYPE     AS     MTRL_TYPE_DESC_TXT
,MBRSH_MARA     AS     INDST_SCTR_LKP_ID
,MBBEZ_T137T_INDUSTRY_SECTOR     AS     INDST_SCTR_DESC_TXT
,MATKL_MARA     AS     MTRL_GROUP_LKP_ID
,WGBEZ_T023T_MATERIAL_GROUP     AS     MTRL_GROUP_DESC_TXT
,MEINS_MARA     AS     BASE_UOM_LKP_ID
,BASE_MSEHL_T006A_UNIT_OF_MEASURE     AS     BASE_UOM_DESC_TXT
,BSTME_MARA     AS     PURCH_ORDER_UOM_LKP_ID
,PURCH_ORDER_MSEHL_T006A_UNIT_OF_MEASURE     AS     PURCH_ORDER_UOM_DESC_TXT
,GROES_MARA     AS     SIZE_DIM_TXT
,BRGEW_MARA     AS     GROS_WEIGHT_NUM
,NTGEW_MARA     AS     NET_WEIGHT_NUM
,GEWEI_MARA     AS     WEIGHT_UOM_LKP_ID
,WEIGHT_MSEHL_T006A_UNIT_OF_MEASURE     AS     WEIGHT_UOM_DESC_TXT
,VOLUM_MARA     AS     VOLUME_NUM
,VOLEH_MARA     AS     VOLUME_UOM_LKP_ID
,VOLUME_MSEHL_T006A_UNIT_OF_MEASURE     AS     VOLUME_UOM_DESC_TXT
,RAUBE_MARA     AS     STRG_CNDTN_CDE
,RBTXT_T142T_STORAGE_CONDITION     AS     STRG_CNDTN_DESC_TXT
,TEMPB_MARA     AS     TMPTR_CNDTN_CDE
,TBTXT_T143T_TEMPERATURE_CONDITION     AS     TMPTR_CNDTN_DESC_TXT
,CALC_REFRIG_FLG     AS     CALC_REFRIG_FLG
,DISST_MARA     AS     LOW_LEVEL_CDE_TXT
,TRAGR_MARA     AS     TRNSPT_GROUP_CDE
,VTEXT_TTGRT_TRANSPORTATION_GROUP     AS     TRNSPT_GROUP_DESC_TXT
,SPART_MARA     AS     DIV_ID
,CALC_NDC_ID     AS     CALC_NDC_ID
,CALC_NDC_CTGRY_ID     AS     CALC_NDC_CTGRY_ID
,CALC_EACH_UPC_ID     AS     CALC_EACH_UPC_ID
,CALC_EACH_UPC_CTGRY_ID     AS     CALC_EACH_UPC_CTGRY_ID
,CALC_CSE_UPC_ID     AS     CALC_CSE_UPC_ID
,CALC_CSE_UPC_CTGRY_ID     AS     CALC_CSE_UPC_CTGRY_ID
,CALC_CTN_UPC_ID     AS     CALC_CTN_UPC_ID
,CALC_CTN_UPC_CTGRY_ID     AS     CALC_CTN_UPC_CTGRY_ID
,CALC_EACH_EAN_ID     AS     CALC_EACH_EAN_ID
,CALC_CSE_EAN_ID     AS     CALC_CSE_EAN_ID
,CALC_CTN_EAN_ID     AS     CALC_CTN_EAN_ID
,CALC_EACH_GTIN_ID     AS     CALC_EACH_GTIN_ID
,CALC_CSE_GTIN_ID     AS     CALC_CSE_GTIN_ID
,CALC_CTN_GTIN_ID     AS     CALC_CTN_GTIN_ID
,LAENG_MARA     AS     LGTH_NUM
,BREIT_MARA     AS     WIDTH_NUM
,HOEHE_MARA     AS     HEIGHT_NUM
,MEABM_MARA     AS     DIM_UOM_LKP_ID
,DIM_MSEHL_T006A_UNIT_OF_MEASURE     AS     DIM_UOM_DESC_TXT
,PRDHA_MARA     AS     PROD_HRCHY_LKP_ID
,VTEXT_T179T_PRODUCT_HIERARCHY     AS     PROD_HRCHY_DESC_TXT
,VABME_MARA     AS     VRBL_PURCH_ORDER_UNIT_ACT_LKP_ID
,VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT     AS     VRBL_PURCH_ORDER_UNIT_ACT_DESC_TXT
,VHART_MARA     AS     PMTL_TYPE_LKP_ID
,VTEXT_TVTYT_PACKAGING_MATERIAL_TYPE     AS     PMTL_TYPE_DESC_TXT
,MAGRV_MARA     AS     MTRL_GROUP_PACK_MTRL_LKP_ID
,BEZEI_TVEGRT_MTRL_GRP_PACKAGING_MATERIALS     AS     MTRL_GROUP_PACK_MTRL_DESC_TXT
,EXTWG_MARA     AS     EXT_MTRL_GROUP_ID
,MSTAE_MARA     AS     XPLNT_MTRL_STAT_LKP_ID
,MTSTB_T141T_CROSS_PLANT_MATERIAL_STATUS     AS     XPLNT_MTRL_STAT_DESC_TXT
,MSTAV_MARA     AS     XDC_MTRL_STAT_LKP_ID
,VMSTB_TVMST_CROSS_DISTRIB_CHAIN_MTRL_STATUS     AS     XDC_MTRL_STAT_DESC_TXT
,MSTDE_MARA     AS     XPLNT_MTRL_STAT_VALID_FROM_DTE
,MSTDV_MARA     AS     XDC_STAT_VALID_FROM_DTE
,MHDRZ_MARA     AS     MIN_RMN_SHELF_LIFE_NUM
,KZUMW_MARA     AS     ENVR_RLVNT_FLG
,KOSCH_MARA     AS     PROD_ALLCT_LKP_ID
,VTEXT_T190ST_PRODUCT_ALLOCATION     AS     PROD_ALLCT_DESC_TXT
,MFRPN_MARA     AS     MFG_PART_ID
,MFRNR_MARA     AS     MFG_NUM
,PROFL_MARA     AS     DGNR_GOOD_IND_PRFL_LKP_ID
,PROFLD_TDG42_DANGEROUS_GOODS_INDICATOR     AS     DGNR_GOODS_IND_PRFL_DESC_TXT
,MTPOS_MARA_MARA     AS     ITEM_CTGRY_GROUP_LKP_ID
,BEZEI_TPTMT_ITEM_ITEM_CATEGORY_GROUP     AS     ITEM_CTGRY_GROUP_DESC_TXT
,SLED_BBD_MARA     AS     EXP_DTE_IND_ID
,EXP_DTE_IND_DESC_TXT     AS     EXP_DTE_IND_DESC_TXT
,CALC_GEN_OTC_FLG     AS     CALC_GEN_OTC_FLG
,CALC_RX_FLG     AS     CALC_RX_FLG
,CALC_DEA_SCHED_ID     AS     CALC_DEA_SCHED_ID
,BEZEI_T606V_LEGAL_CONTROL     AS     DEA_SCHED_NUM_DESC_TXT
,YYBASE_MARA     AS     DEA_BASE_CDE_LKP_ID
,YYDESC_YTMM_DEA_TEXT_DEA_BASE_CODE     AS     DEA_BASE_CDE_DESC_TXT
,YYSUBB_MARA     AS     DEA_SUB_BASE_CDE_LKP_ID
,YYDESC_YTMM_SUBBASETEXT_DEA_SUB_BASE_CODE     AS     DEA_SUB_BASE_CDE_DESC_TXT
,YYLDESC_MARA     AS     LONG_MTRL_DESC_TXT
,YYAHFS_MARA     AS     AHFS_LKP_ID
,YYDESC_YTMM_AHFS_TEXT_AHFS_CODE     AS     AHFS_DESC_TXT
,YYLSTO_MARA     AS     LIST_1_CHEM_LKP_ID
,YYDESC_YTMM_LIST1_TEXT_LIST_1_CHEMICAL     AS     LIST_1_CHEM_DESC_TXT
,YYFORM_MARA     AS     FORM_LKP_ID
,YYDESC_YTMM_FORM_TEXT_FORM     AS     FORM_DESC_TXT
,YYCGCN_MARA     AS     GEN_CDE_NUM
,YYGENN_MARA     AS     GEN_NAM_TXT
,YYSTRN_MARA     AS     STRGTH_TXT
,YYPCKG_MARA     AS     PACK_IND_LKP_ID
,YYDESC_YTMM_PACK_TEXT_PACKAGING_INDICATOR     AS     PACK_IND_DESC_TXT
,UNIT_DOSE_FLG     AS     UNIT_DOSE_FLG
,YYSUPNR_MARA     AS     SUPPLIER_NUM
,NAME1_LFA1     AS     SUPPLIER_NAM
,YYHAZSC_MARA     AS     HAZMAT_STRG_LKP_ID
,YYDESC_YTMM_HAZMAT_TEXT_HAZMAT_STORAGE     AS     HAZMAT_STRG_DESC_TXT
,YYSPHD_MARA     AS     SPECIAL_HNDL_LKP_ID
,YYDESC_YTMM_SPECL_TEXT_SPECIAL_HANDLING     AS     SPECIAL_HNDL_DESC_TXT
,YYINJECT_MARA     AS     INJCT_FLG
,YYFINE_MARA     AS     HAM_FINE_CLASS_LKP_ID
,YYDESC_YTMM_FINE_TEXT_HAMACHER_FINE_CLASS     AS     HAM_FINE_CLASS_DESC_TXT
,YYFINER_MARA     AS     HAM_FINER_CLASS_CDE_LKP_ID
,YYDESC_YTMM_FINER_TEXT_HAMACHER_FINER_CLASS     AS     HAM_FINER_CLASS_DESC_TXT
,YYFINEST_MARA     AS     HAM_FINEST_CLASS_LKP_ID
,YYDESC_YTMM_FINEST_TEXT_HAMACHER_FINEST_CLASS     AS     HAM_FINEST_CLASS_DESC_TXT
,YYFLASH_MARA     AS     FLSH_POINT_TXT
,YYTNAM_MARA     AS     TECH_NAM_TXT
,YYCASN1_MARA     AS     CAS_1_NUM
,YYCASP1_MARA     AS     CAS_1_PCT
,YYCASN2_MARA     AS     CAS_2_NUM
,YYCASP2_MARA     AS     CAS_2_PCT
,YYCASN3_MARA     AS     CAS_3_NUM
,YYCASP3_MARA     AS     CAS_3_PCT
,YYCASN4_MARA     AS     CAS_4_NUM
,YYCASP4_MARA     AS     CAS_4_PCT
,YYCASN5_MARA     AS     CAS_5_NUM
,YYCASP5_MARA     AS     CAS_5_PCT
,YYCASN6_MARA     AS     CAS_6_NUM
,YYCASP6_MARA     AS     CAS_6_PCT
,YYDOTSP_MARA     AS     DOT_SPECIAL_PRMT_TXT
,YYCMPTN_MARA     AS     CMPR_TO_TXT
,YYHAZKEY_MARA     AS     HAZMAT_KEY_NUM
,YYGCNSQN_MARA     AS     GCN_SEQ_NUM
,YYBOXSTYLE_MARA     AS     BOX_STYL_ID
,YYCHG3P_MARA     AS     THIRD_PARTY_DATA_CHG_NUM
,YYLOGGR_MARA     AS     MTRL_PTWY_ID
,YYPACREF_MARA     AS     PACK_RFRNC_GUIDE_LKP_ID
,YYDESC_YTMM_PACREF_TEXT_PACKAGE_REFERENCE     AS     PACK_RFRNC_GUIDE_DESC_TXT
,YYFORMFAM_MARA     AS     FORM_FMLY_ID
,YYNDCFDB_MARA     AS     FDB_NDC_CDE
,YYJCODEFDB_MARA     AS     FDB_J_CDE
,YYJCODECAH_MARA     AS     CAH_J_CDE
,YYHCPCFDB_MARA     AS     FDB_HCPC_CDE
,YYHCPCCAH_MARA     AS     CAH_HCPC_CDE
,YYHCPCDESC_MARA     AS     HCPC_DESC_TXT
,YYHCPCDOSVAL_MARA     AS     HCPC_DOSAGE_VALUE_NUM
,YYHCPCDOSUOM_MARA     AS     HCPC_DOSAGE_UOM_ID
,YYPAYLIMFDB_MARA     AS     FDB_PYMT_LMT_AMT
,YYSTNGSP_MARA     AS     SPECIALTY_STRGTH_VALUE_NUM
,YYSTNGUOMSP_MARA     AS     SPECIALTY_STRGTH_UOM_ID
,YYVOLSP_MARA     AS     SPECIALTY_VOLUME_VALUE_NUM
,YYVOLUOMSP_MARA     AS     SPECIALTY_VOLUME_UOM_ID
,YYCONCNUM_MARA     AS     SPECIALTY_CNCTRT_NMRTR_VALUE_NUM
,YYCONCNUMUOM_MARA     AS     SPECIALTY_CNCTRT_NMRTR_UOM_ID
,YYCONCDEN_MARA     AS     SPECIALTY_CNCTRT_DMNTR_VALUE_NUM
,YYCONCDENUOM_MARA     AS     SPECIALTY_CNCTRT_DMNTR_UOM_ID
,YYBILLVLSP_MARA     AS     SPECIALTY_BILL_UNIT_VIAL_NUM
,YYBILLPKSP_MARA     AS     SPECIALTY_BILL_UNIT_PACK_NUM
,YYTAIDISP_MARA     AS     SPECIALTY_TOTAL_ACTIVE_INGRED_DISP_NUM
,YYTAIPRCH_MARA     AS     SPECIALTY_TOTAL_ACTIVE_INGRED_PURCH_NUM
,YYTAIUOM_MARA     AS     SPECIALTY_TOTAL_ACTIVE_INGRED_UOM_ID
,YYSTKRQTY_MARA     AS     STICKER_QTY
,YYTRADNAME_MARA     AS     TRADE_NAM
,YYCONKEY_MARA     AS     CNSMR_KEY_NUM
,YYSRCSKEY_MARA     AS     SRC_SUB_KEY_ID
,YYGCNSQNC_MARA     AS     CARD_GCN_SEQ_NUM
,YYAHFSC_MARA     AS     CARD_AHFS_CDE
,CARD_AHFS_CDE_DESC_TXT     AS     CARD_AHFS_CDE_DESC_TXT
,YYOBRAT_MARA     AS     FDB_OBC_LKP_ID
,FDB_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE     AS     FDB_OBC_DESC_TXT
,YYPKGKEY_MARA     AS     PACK_KEY_ID
,YYPKGKEYC_MARA     AS     CAH_PACK_KEY_ID
,YYREMSP_MARA     AS     REMS_PROG_LKP_ID
,YYDESC_YTMM_REMS_PROG_T_REMS_PROGRAM     AS     REMS_PROG_DESC_TXT
,YYPKSZE_MARA     AS     PACK_SIZE_NUM
,CALC_PACK_QTY     AS     CALC_PACK_QTY
,CALC_ACCUNET_QTY     AS     CALC_ACCUNET_QTY
,YYMLTSRCF_MARA     AS     FDB_MULTI_SRC_IND_ID
,YYAWPM_MARA     AS     MDSP_AWP_AMT
,YYUOMM_MARA     AS     MDSP_UOM_ID
,YYPRIU_MARA     AS     PRICE_USAGE_LKP_ID
,YYDESC_YTMM_PRIU_T_PRICING_USAGE     AS     PRICE_USAGE_DESC_TXT
,YYEDESC_MARA     AS     EXPANDED_MTRL_DESC_TXT
,YYOBRATC_MARA     AS     CARD_OBC_LKP_ID
,CARD_YYDESC_YTMM_OBRAT_TEXT_ORANGE_BOOK_CODE     AS     CARD_OBC_DESC_TXT
,YYREQG_MARA     AS     REQ_GROUP_CDE_ID
,YYMANOVR_MARA     AS     MANUAL_OVRD_FLG
,YYOVRREA_MARA     AS     OVRD_REASON_TXT
,ZZTAXD_MARA     AS     SALE_TAX_DRVR_LKP_ID
,DESCRIPTION_ZMT_PTS_TAX_DRVR_SALES_TAX_DRIVER     AS     SALE_TAX_DRVR_DESC_TXT
,ZZCALPROP65_MARA     AS     CA_PRPS_65_ID
,YYOPIOID_MARA     AS     OPIOID_FLG
,YYDEANAME_MARA     AS     DEA_NAM_TXT
,YYNIOSH_MARA     AS     NIOSH_KEY_ID
,CALC_CARD_SBST_KEY_ID     AS     CALC_CARD_SBST_KEY_ID
,CALC_CSE_QTY     AS     CALC_CSE_QTY
,CALC_DOSE_QTY     AS     CALC_DOSE_QTY
,CALC_CTN_QTY     AS     CALC_CTN_QTY
,CALC_EACH_QTY     AS     CALC_EACH_QTY
,CALC_RMBMT_UOM_CDE     AS     CALC_RMBMT_UOM_CDE
,CALC_RMBMT_UOM_TXT     AS     CALC_RMBMT_UOM_TXT
,CALC_RMBMT_UOM_QTY     AS     CALC_RMBMT_UOM_QTY
,CALC_SHAPE     AS     SHAPE_TXT
,CALC_COLOR     AS     COLOR_TXT
,CALC_FLAVOR     AS     FLAVOR_TXT
,DRUG_GROUP_ID_MEDISPAN_PROD     AS     MDSP_DRUG_GROUP_ID
,DRUG_GROUP_DESC_MEDISPAN     AS     MDSP_DRUG_GROUP_DESC_TXT
,DRUG_CLASS_ID_MEDISPAN_PROD     AS     MDSP_DRUG_CLASS_ID
,DRUG_CLASS_DESC_MEDISPAN     AS     MDSP_DRUG_CLASS_DESC_TXT
,DRUG_SUBCLS_ID_MEDISPAN_PROD     AS     MDSP_DRUG_SUBCLS_ID
,DRUG_SUBCLASS_DESC_MEDISPAN     AS     MDSP_DRUG_SUBCLS_DESC_TXT
,GEN_PROD_ID_ARCH_MEDISPAN     AS     MDSP_GEN_PROD_ID
,TEE_ID_ARCH_MEDISPAN     AS     MDSP_THER_EQV_ID
,YYBUNIT_MARA AS  BILL_UOM_ID
,BILL_UNIT_MSEHL_T006A_UNIT_OF_MEASURE AS BILL_UOM_DESC_TXT
,BILL_UNIT_MSEHT_T006A_UNIT_OF_MEASURE AS BILL_UOM_SHRT_DESC_TXT
,CALC_DSCSA_EXEMPT_ID     AS     CALC_DSCSA_EXEMPT_ID
,CALC_FOOD_FLG     AS     CALC_FOOD_FLG
,CALC_EXP_DTE_FLG     AS     CALC_EXP_DTE_FLG
,CALC_VETERINARY_ID     AS     CALC_VETERINARY_ID
,CALC_CONTAINS_BPA_ID     AS     CALC_CONTAINS_BPA_ID
,CALC_CONTAINS_DEHP_ID     AS     CALC_CONTAINS_DEHP_ID
,CALC_EPA_REGISTERED_ID     AS     CALC_EPA_REGISTERED_ID
,CALC_ETO_STERILIZED_ID     AS     CALC_ETO_STERILIZED_ID
,CALC_DIR_SRC_FLG     AS     CALC_DIR_SRC_FLG
,CALC_SERIAL_FLG     AS     CALC_SERIAL_FLG
,CALC_MED_FOOD_FLG     AS     CALC_MED_FOOD_FLG
,CALC_KIT_WITH_DRUG_FLG     AS     CALC_KIT_WITH_DRUG_FLG
,CALC_CA_PRPS_65_ID     AS     CALC_CA_PRPS_65_ID
,CALC_PLASMA_ID     AS     CALC_PLASMA_ID
,CALC_TRADE_PRTNR_TYPE_ID     AS     CALC_TRADE_PRTNR_TYPE_ID
,CALC_NY_LAW_CDE_ID     AS     CALC_NY_LAW_CDE_ID
,CALC_ADDL_MFG_INFO_FLG     AS     CALC_ADDL_MFG_INFO_FLG
,CALC_BONUS_PACK_MTRL_NUM     AS     CALC_BONUS_PACK_MTRL_NUM
,CALC_PURCH_BLCK_REVIEW_NEED_FLG     AS     CALC_PURCH_BLCK_REVIEW_NEED_FLG
,CALC_STICKERS_ONLY_FLG     AS     CALC_STICKERS_ONLY_FLG
,CALC_BRAND_GEN_EXCP_ID     AS     CALC_BRAND_GEN_EXCP_ID
,CALC_DEMAND_FORECASTING_ALRG_FLG     AS     CALC_DEMAND_FORECASTING_ALRG_FLG
,CALC_DEMAND_FORECASTING_COUGH_COLD_FLG     AS     CALC_DEMAND_FORECASTING_COUGH_COLD_FLG
,CALC_FLU_ID     AS     CALC_FLU_ID
--,CALC_DOCUMENTS_MSNG_ID     AS     CALC_DOCUMENTS_MSNG_ID
--,CALC_DPLCT_MTRL_REASON_ID     AS     CALC_DPLCT_MTRL_REASON_ID
,CALC_MEDSURG_FLG     AS     CALC_MEDSURG_FLG
,CALC_MEDSURG_ID     AS     CALC_MEDSURG_ID
,CALC_COVID_ID     AS     CALC_COVID_ID
,CALC_KINRAY_ITEM_FLG     AS     CALC_KINRAY_ITEM_FLG
,CALC_PUERTO_RICO_ITEM_ID     AS     CALC_PUERTO_RICO_ITEM_ID
,CALC_BIOLOGIC_BLA_TYPE_ID     AS     CALC_BIOLOGIC_BLA_TYPE_ID
,CALC_IB_SPECIAL_PROCESS_FLG     AS     CALC_IB_SPECIAL_PROCESS_FLG
,CALC_CNSMR_HLTH_PLNGRM_FLG AS CALC_CNSMR_HLTH_PLNGRM_FLG
,CALC_VACCINE_PORTAL_ID AS CALC_VACCINE_PORTAL_ID
,COMPOSITE_KEY AS COMPOSITE_KEY
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
,CALC_BASE_UOM_UPC_ID AS CALC_BASE_UOM_UPC_ID
,CALC_D0_START_STP AS CALC_D0_START_STP
,ENTERED_DTE_ITEM_MAST AS ENTERED_DTE
,CALC_MED_INTGRT_DSPNS_FLG AS CALC_MED_INTGRT_DSPNS_FLG
,CALC_ECOM_PROD_CTLG_FLG AS CALC_ECOM_PROD_CTLG_FLG
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_CV`;