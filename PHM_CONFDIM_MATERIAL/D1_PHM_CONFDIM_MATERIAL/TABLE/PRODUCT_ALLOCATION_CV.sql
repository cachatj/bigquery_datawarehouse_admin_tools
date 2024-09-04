CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PRODUCT_ALLOCATION_CV`
(
  KOSCH_T190ST STRING OPTIONS(description="Product allocation ID; T190ST.KOSCH where T190ST.LANGU = E"),
  VTEXT_T190ST STRING OPTIONS(description="Product allocation Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY KOSCH_T190ST;