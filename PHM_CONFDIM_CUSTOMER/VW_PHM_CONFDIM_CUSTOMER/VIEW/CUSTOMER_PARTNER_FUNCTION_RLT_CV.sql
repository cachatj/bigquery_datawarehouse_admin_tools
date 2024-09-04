CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT_CV`(
  CUSTOMER_NUM OPTIONS(description="Customer Number,"),
  SALE_ORG_ID OPTIONS(description="Sales Organization ID,"),
  DIST_CHNL_ID OPTIONS(description="Distribution Channel ID,"),
  DIV_ID OPTIONS(description="Division ID,"),
  PRTNR_FNCTN_ID OPTIONS(description="Partner Function ID,"),
  VRSN_START_DTE OPTIONS(description="Derived by data engineering,"),
  VRSN_END_DTE OPTIONS(description="Derived by data engineering,"),
  CURR_VRSN_FLG OPTIONS(description="Derived by data engineering,"),
  PRTNR_FNCTN_DESC_TXT OPTIONS(description="Partner Function Description Text"),
  PRTNR_FNCTN_CUSTOMER_NUM OPTIONS(description="Partner Function Customer Number,"),
  PRTNR_COUNT_NUM OPTIONS(description="Partner Counter Number"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KUNNR_KNVP     AS     CUSTOMER_NUM
,VKORG_KNVP     AS     SALE_ORG_ID
,VTWEG_KNVP     AS     DIST_CHNL_ID
,SPART_KNVP     AS     DIV_ID
,PARVW_KNVP     AS     PRTNR_FNCTN_ID
,VRSN_START_DTE     AS     VRSN_START_DTE
,VRSN_END_DTE     AS     VRSN_END_DTE
,CURR_VRSN_FLG     AS     CURR_VRSN_FLG
,VTEXT_TPART_PARTNER_FUNCTION_CODE     AS     PRTNR_FNCTN_DESC_TXT
,KUNN2_KNVP     AS     PRTNR_FNCTN_CUSTOMER_NUM
,PARZA_KNVP     AS     PRTNR_COUNT_NUM
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT_CV`;