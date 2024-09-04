CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV`(
  PMTL_TYPE_ID OPTIONS(description="Packaging Material Type ID"),
  PMTL_TYPE_DESC_TXT OPTIONS(description="Packaging Material Type Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 TRATY_TVTYT     AS     PMTL_TYPE_ID
,VTEXT_TVTYT     AS     PMTL_TYPE_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV`;