CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_CV`(
  SHELF_LABEL_ID OPTIONS(description="Shelf Label ID"),
  SHELF_LABEL_DESC_TXT OPTIONS(description="Shelf Label Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYSHELFLAB_YTSD_SHELFLABTXT     AS     SHELF_LABEL_ID
,YYSHELFLAB_DES_YTSD_SHELFLABTXT     AS     SHELF_LABEL_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_CV`;