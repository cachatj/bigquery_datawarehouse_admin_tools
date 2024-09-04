CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.CROSS_PLANT_MATERIAL_STATUS_CV`
(
  MMSTA_T141T STRING OPTIONS(description="Cross Plant Material Status ID; T141T.MMSTA where T141T.SPRAS = E"),
  MTSTB_T141T STRING OPTIONS(description="Cross Plant Material Status Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MMSTA_T141T;