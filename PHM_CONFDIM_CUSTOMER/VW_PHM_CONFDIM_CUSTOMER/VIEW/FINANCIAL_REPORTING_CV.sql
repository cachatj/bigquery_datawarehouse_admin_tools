CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.FINANCIAL_REPORTING_CV`(
  FNC_RPT_ID OPTIONS(description="Financial Reporting ID"),
  FNC_RPT_DESC_TXT OPTIONS(description="Financial Reporting Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KVGR5_TVV5T     AS     FNC_RPT_ID
,BEZEI_TVV5T     AS     FNC_RPT_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.FINANCIAL_REPORTING_CV`;