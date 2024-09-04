CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.INDUSTRY_SECTOR_CV`
(
  MBRSH_T137T STRING OPTIONS(description="Industry Sector ID; T137T.MBRSH where T137T.SPRAS = E"),
  MBBEZ_T137T STRING OPTIONS(description="Industry Sector Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MBRSH_T137T;