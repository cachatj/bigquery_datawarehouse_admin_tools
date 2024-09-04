CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV`(
  PRIM_BKORDR_SELECT_ID OPTIONS(description="Primary Backorder Selection ID"),
  PRIM_BKORDR_SELECT_DESC_TXT OPTIONS(description="Primary Backorder Selection Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYBOPRE1_YTSD_BOPRE1_TEXT     AS     PRIM_BKORDR_SELECT_ID
,YYBOPRE1_DES_YTSD_BOPRE1_TEXT     AS     PRIM_BKORDR_SELECT_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV`;