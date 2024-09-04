CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.REMS_PROGRAM_CV`(
  REMS_PROG_ID OPTIONS(description="REMS Program ID"),
  REMS_PROG_DESC_TXT OPTIONS(description="REMS Program Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 YYREMSP_YTMM_REMS_PROG_T     AS     REMS_PROG_ID
,YYDESC_YTMM_REMS_PROG_T     AS     REMS_PROG_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.REMS_PROGRAM_CV`;