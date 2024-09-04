CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CONSIGNMENT_PROGRAM_CV`(
  CONSIGN_PROG_ID OPTIONS(description="Consignment Program ID"),
  CONSIGN_PROG_DESC_TXT OPTIONS(description="Consignment Program Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYCONSIGN_YTSD_CONSIGN_TXT     AS     CONSIGN_PROG_ID
,YYCONSIGN_DES_YTSD_CONSIGN_TXT     AS     CONSIGN_PROG_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CONSIGNMENT_PROGRAM_CV`;