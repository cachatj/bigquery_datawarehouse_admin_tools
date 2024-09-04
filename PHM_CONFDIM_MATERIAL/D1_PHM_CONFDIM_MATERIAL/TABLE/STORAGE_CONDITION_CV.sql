CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.STORAGE_CONDITION_CV`
(
  RAUBE_T142T STRING OPTIONS(description="Storage conditions ID; T142T.RAUBE where T142T.SPRAS = E"),
  RBTXT_T142T STRING OPTIONS(description="Storage conditions Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY RAUBE_T142T;