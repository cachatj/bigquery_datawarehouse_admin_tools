CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.HAMACHER_FINE_CLASS_CV`(
  HAM_FINE_CLASS_ID OPTIONS(description="Hamacher Fine Class ID"),
  HAM_FINE_CLASS_DESC_TXT OPTIONS(description="Hamacher Fine Class Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 YYFINE_YTMM_FINE_TEXT     AS     HAM_FINE_CLASS_ID
,YYDESC_YTMM_FINE_TEXT     AS     HAM_FINE_CLASS_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.HAMACHER_FINE_CLASS_CV`;