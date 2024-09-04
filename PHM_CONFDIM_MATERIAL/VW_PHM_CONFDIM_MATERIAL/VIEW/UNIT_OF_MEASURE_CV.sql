CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.UNIT_OF_MEASURE_CV`(
  INTRNL_UOM_ID OPTIONS(description="Internal Unit Of Measure ID"),
  EXTRNL_CMRCL_UOM_ID OPTIONS(description="External Commercial Unit Of Measure ID"),
  UOM_DESC_TXT OPTIONS(description="Unit Of Measure Description Text"),
  UOM_SHRT_DESC_TXT OPTIONS(description="Unit Of Measure Short Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 MSEHI_T006A     AS     INTRNL_UOM_ID
,MSEH3_T006A     AS     EXTRNL_CMRCL_UOM_ID
,MSEHL_T006A     AS     UOM_DESC_TXT
,MSEHT_T006A     AS     UOM_SHRT_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.UNIT_OF_MEASURE_CV`;