CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV`
(
  TRATY_TVTYT STRING OPTIONS(description="Packaging Material Type ID; TVTYT.TRATY where TVTYT.SPRAS = E"),
  VTEXT_TVTYT STRING OPTIONS(description="Packaging Material Type Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY TRATY_TVTYT;