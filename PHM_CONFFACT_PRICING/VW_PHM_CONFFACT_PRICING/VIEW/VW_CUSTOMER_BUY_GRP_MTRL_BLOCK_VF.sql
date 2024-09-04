CREATE VIEW `PROJECT_ID.VW_PHM_CONFFACT_PRICING.VW_CUSTOMER_BUY_GRP_MTRL_BLOCK_VF`(
  AGR_NUM_ID OPTIONS(description="Aggrement Number"),
  SALE_ORG_ID OPTIONS(description="Sales Organization"),
  SALE_ORG_DESC_TXT OPTIONS(description="Sales Organization Description Text"),
  DIST_CHNL_ID OPTIONS(description="Distribute Channel"),
  DIST_CHNL_DESC_TXT OPTIONS(description="Distribution Channel Description Text"),
  DIV_ID OPTIONS(description="Division"),
  DIV_DESC_TXT OPTIONS(description="Division Description Text"),
  SOLDTO_CUSTOMER_NUM_ID OPTIONS(description="Sold To Customer Number"),
  BUY_GROUP_ID OPTIONS(description="Buying Group"),
  MTRL_NUM_ID OPTIONS(description="Material Number"),
  VRSN_START_DTE OPTIONS(description="Version Start Date"),
  VRSN_END_DTE OPTIONS(description="Version End Date"),
  CURR_VRSN_FLG OPTIONS(description="Current Version Flag"),
  DEL_FLG OPTIONS(description="Deletion Indicator Flag"),
  ROW_ADD_STP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Added By User"),
  ROW_UPDATE_STP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Updated By User")
)
AS SELECT
  DISTINCT
KNUMA_AG_A716  AS AGR_NUM_ID
,VKORG_A716 AS SALE_ORG_ID
,VTEXT_TVKOT AS SALE_ORG_DESC_TXT
,VTWEG_A716 AS DIST_CHNL_ID
,VTEXT_TVTWT AS DIST_CHNL_DESC_TXT
,SPART_A716 AS DIV_ID
,VTEXT_TSPAT AS DIV_DESC_TXT
,KUNAG_A716 AS SOLDTO_CUSTOMER_NUM_ID
,IRM_PCBGP_PRI_A716 AS BUY_GROUP_ID
,MATNR_A716 AS MTRL_NUM_ID
,DATAB_A716 AS VRSN_START_DTE
,DATBI_A716 AS VRSN_END_DTE
,CURR_VRSN_FLG AS CURR_VRSN_FLG
,LOEVM_KO_KONP AS DEL_FLG
,ROW_ADD_STP
,ROW_ADD_USER_ID
,ROW_UPDATE_STP
,ROW_UPDATE_USER_ID

FROM `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_BUY_GRP_MTRL_BLOCK_VF`;