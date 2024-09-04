CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.UNIT_OF_MEASURE_CV`
(
  MSEHI_T006A STRING OPTIONS(description="Internal Unit Of Measure ID; T006A.MSEHI where T006A.SPRAS = E"),
  MSEH3_T006A STRING OPTIONS(description="External Commercial Unit Of Measure ID"),
  MSEHL_T006A STRING OPTIONS(description="Unit Of Measure Description Text"),
  MSEHT_T006A STRING OPTIONS(description="Unit Of Measure Short Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MSEHI_T006A, MSEH3_T006A;