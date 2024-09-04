CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_SALES_AREA_RLT_CV`(
  MTRL_NUM OPTIONS(description="Material Number"),
  SALE_ORG_ID OPTIONS(description="Sales Organization"),
  DIST_CHNL_ID OPTIONS(description="Distribution Channel"),
  DIST_CHAIN_DEL_IND_FLG OPTIONS(description="Distribution Chain Deletion Indicator Flag"),
  DIST_CHAIN_SPCFC_MTRL_STAT_ID OPTIONS(description="Distribution Chain Specific Material Status ID"),
  DIST_CHAIN_SPCFC_MTRL_STAT_DESC_TXT OPTIONS(description="Distribution Chain Specific Material Status Description Text"),
  DIST_CHAIN_SPCFC_MTRL_STAT_VALID_FROM_DTE OPTIONS(description="Distribution Chain Specific Material Status Valid From Date"),
  ITEM_CTGRY_GROUP_ID OPTIONS(description="Item Category Group ID"),
  ITEM_CTGRY_GROUP_DESC_TXT OPTIONS(description="Item Category Group Description Text"),
  MTRL_PRICE_GROUP_ID OPTIONS(description="Material Pricing Group ID"),
  MTRL_ACCT_ASGNMT_GROUP_ID OPTIONS(description="Material Account Assignment Group (AAG) ID"),
  WEB_ENABLE_FLG OPTIONS(description="Web Enabled Flag"),
  SFDC_ENABLE_FLG OPTIONS(description="SFDC Enabled Flag"),
  REPLC_MTRL_NUM OPTIONS(description="Replacement Material Number"),
  CLOSEST_ALTRNT_MTRL_NUM OPTIONS(description="Closest Alternative Material Number"),
  FRMLRY_SUBSTITUTE_MTRL_NUM OPTIONS(description="Formulary Substitute Material Number"),
  ROW_ADD_STP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
OPTIONS(
  description=""
)
AS SELECT
	MATNR_MVKE        		AS MTRL_NUM,
	VKORG_MVKE        		AS SALE_ORG_ID,
	VTWEG_MVKE        		AS DIST_CHNL_ID,
	LVORM_MVKE        		AS DIST_CHAIN_DEL_IND_FLG,
	VMSTA_MVKE        		AS DIST_CHAIN_SPCFC_MTRL_STAT_ID,
	VMSTB_TVMST       		AS DIST_CHAIN_SPCFC_MTRL_STAT_DESC_TXT,
	VMSTD_MVKE        		AS DIST_CHAIN_SPCFC_MTRL_STAT_VALID_FROM_DTE,
	MTPOS_MVKE        		AS ITEM_CTGRY_GROUP_ID,
	BEZEI_TPTMT       		AS ITEM_CTGRY_GROUP_DESC_TXT,
	KONDM_MVKE        		AS MTRL_PRICE_GROUP_ID,
	KTGRM_MVKE        		AS MTRL_ACCT_ASGNMT_GROUP_ID,
	PRAT1_MVKE        		AS WEB_ENABLE_FLG,
	PRAT3_MVKE        		AS SFDC_ENABLE_FLG,
	YYRMAT_MVKE       		AS REPLC_MTRL_NUM,
	YYCMAT_MVKE       		AS CLOSEST_ALTRNT_MTRL_NUM,
	YYFMAT_MVKE       		AS FRMLRY_SUBSTITUTE_MTRL_NUM,
	ROW_ADD_STP       		AS ROW_ADD_STP,
	ROW_ADD_USER_ID   		AS ROW_ADD_USER_ID,
	ROW_UPDATE_STP    		AS ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID		AS ROW_UPDATE_USER_ID
FROM
   PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_SALES_AREA_RLT_CV;