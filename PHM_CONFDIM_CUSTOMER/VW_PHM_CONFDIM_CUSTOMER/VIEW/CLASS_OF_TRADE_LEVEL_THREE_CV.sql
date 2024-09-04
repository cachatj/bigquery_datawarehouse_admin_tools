CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CLASS_OF_TRADE_LEVEL_THREE_CV`(
  CLASS_OF_TRADE_LEVEL_3_ID OPTIONS(description="Class Of Trade Level 3 ID"),
  CLASS_OF_TRADE_LEVEL_3_DESC_TXT OPTIONS(description="Class Of Trade Level 3 Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KVGR1_TVV1T     AS     CLASS_OF_TRADE_LEVEL_3_ID
,BEZEI_TVV1T     AS     CLASS_OF_TRADE_LEVEL_3_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CLASS_OF_TRADE_LEVEL_THREE_CV`;