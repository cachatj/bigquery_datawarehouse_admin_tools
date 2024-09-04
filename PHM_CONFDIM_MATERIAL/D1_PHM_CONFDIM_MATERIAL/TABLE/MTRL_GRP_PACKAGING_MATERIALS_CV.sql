CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MTRL_GRP_PACKAGING_MATERIALS_CV`
(
  MAGRV_TVEGRT STRING OPTIONS(description="Material Group Packaging Materials ID; TVEGRT.MAGRV where TVEGRT.SPRAS = E"),
  BEZEI_TVEGRT STRING OPTIONS(description="Material Group Packaging Materials Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MAGRV_TVEGRT;