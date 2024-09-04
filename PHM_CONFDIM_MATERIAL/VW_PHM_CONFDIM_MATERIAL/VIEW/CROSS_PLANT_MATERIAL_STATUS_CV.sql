CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.CROSS_PLANT_MATERIAL_STATUS_CV`(
  XPLNT_MTRL_STAT_ID OPTIONS(description="Cross Plant Material Status ID"),
  XPLNT_MTRL_STAT_DESC_TXT OPTIONS(description="Cross Plant Material Status Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 MMSTA_T141T     AS     XPLNT_MTRL_STAT_ID
,MTSTB_T141T     AS     XPLNT_MTRL_STAT_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.CROSS_PLANT_MATERIAL_STATUS_CV`;