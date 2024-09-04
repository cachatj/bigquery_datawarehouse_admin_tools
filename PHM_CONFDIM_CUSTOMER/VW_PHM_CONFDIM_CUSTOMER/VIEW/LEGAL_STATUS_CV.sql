CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.LEGAL_STATUS_CV`(
  LGL_STAT_ID OPTIONS(description="Legal status ID"),
  LGL_STAT_DESC_TXT OPTIONS(description="Legal status Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 GFORM_TVGFT     AS     LGL_STAT_ID
,VTEXT_TVGFT     AS     LGL_STAT_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.LEGAL_STATUS_CV`;