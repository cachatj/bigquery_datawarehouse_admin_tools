CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.SALES_TAX_DRIVER_CV`(
  SALE_TAX_DRVR_ID OPTIONS(description="Sales Tax Driver ID"),
  TAX_GROUP_CDE OPTIONS(description="Tax Group Code"),
  SALE_TAX_DRVR_DESC_TXT OPTIONS(description="Sales Tax Driver Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 ZZMTAXD_ZMT_PTS_TAX_DRVR     AS     SALE_TAX_DRVR_ID
,ZZTAXGC_ZMT_PTS_TAX_DRVR     AS     TAX_GROUP_CDE
,DESCRIPTION_ZMT_PTS_TAX_DRVR     AS     SALE_TAX_DRVR_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SALES_TAX_DRIVER_CV`;