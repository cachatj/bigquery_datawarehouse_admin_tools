CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.DANGEROUS_GOOD_INDICATOR_CV`
(
  PROFL_TDG42 STRING OPTIONS(description="Dangerous Goods Indicator Profile ID; TDG42.PROFL where TDG42.DGSPRAS = E"),
  PROFLD_TDG42 STRING OPTIONS(description="Dangerous Goods Indicator Profile Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY PROFL_TDG42;