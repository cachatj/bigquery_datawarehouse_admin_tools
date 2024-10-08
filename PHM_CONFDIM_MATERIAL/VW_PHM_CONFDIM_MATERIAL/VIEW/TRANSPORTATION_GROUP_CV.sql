CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.TRANSPORTATION_GROUP_CV`(
  TRNSPT_GROUP_ID OPTIONS(description="Transportation Group ID"),
  TRNSPT_GROUP_DESC_TXT OPTIONS(description="Transportation Group Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 TRAGR_TTGRT     AS     TRNSPT_GROUP_ID
,VTEXT_TTGRT     AS     TRNSPT_GROUP_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.TRANSPORTATION_GROUP_CV`;