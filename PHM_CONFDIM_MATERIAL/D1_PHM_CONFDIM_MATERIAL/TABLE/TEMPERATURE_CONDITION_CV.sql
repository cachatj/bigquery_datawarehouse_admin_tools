CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.TEMPERATURE_CONDITION_CV`
(
  TEMPB_T143T STRING OPTIONS(description="Temperature condition ID; T143T.TEMPB where T143T.SPRAS = E"),
  TBTXT_T143T STRING OPTIONS(description="Temperature condition Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY TEMPB_T143T;