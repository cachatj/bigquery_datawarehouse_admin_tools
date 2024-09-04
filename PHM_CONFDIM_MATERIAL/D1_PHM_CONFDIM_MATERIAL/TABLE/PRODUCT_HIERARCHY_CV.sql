CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PRODUCT_HIERARCHY_CV`
(
  PRODH_T179 STRING OPTIONS(description="Product hierarchy ID"),
  STUFE_T179 INT64 OPTIONS(description="Level Number"),
  VTEXT_T179T STRING OPTIONS(description="Product Hierarchy Description Text; T179T.VTEXT where T179.PRODH = T179T.PRODH and T179T.SPRAS = E"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY PRODH_T179;