CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.ITEM_CATEGORY_GROUP_CV`
(
  MTPOS_TPTMT STRING OPTIONS(description="Item Category Group ID; TPTMT.MTPOS where TPTMT.SPRAS = E"),
  BEZEI_TPTMT STRING OPTIONS(description="Item Category Group Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MTPOS_TPTMT;