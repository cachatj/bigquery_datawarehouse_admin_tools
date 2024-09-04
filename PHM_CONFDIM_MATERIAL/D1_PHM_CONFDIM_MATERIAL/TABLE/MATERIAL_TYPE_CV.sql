CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_TYPE_CV`
(
  MTART_T134T STRING OPTIONS(description="Material Type ID; T134T.MTART where T134T.SPRAS = E"),
  MTBEZ_T134T STRING OPTIONS(description="Material type description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MTART_T134T;