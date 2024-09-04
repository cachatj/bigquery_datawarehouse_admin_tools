CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_OPTIONS_CV`(
  SHELF_LABEL_OPTIONS_ID OPTIONS(description="Shelf Label Options ID"),
  SHELF_LABEL_OPTIONS_DESC_TXT OPTIONS(description="Shelf Label Options Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYOESLOPT_KNVV     AS     SHELF_LABEL_OPTIONS_ID
,SHELF_LABEL_OPTIONS_DESC_TXT     AS     SHELF_LABEL_OPTIONS_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_OPTIONS_CV`;