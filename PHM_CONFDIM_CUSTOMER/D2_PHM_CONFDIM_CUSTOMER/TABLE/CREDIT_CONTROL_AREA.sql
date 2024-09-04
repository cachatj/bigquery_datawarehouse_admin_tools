CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CREDIT_CONTROL_AREA`
(
  KKBER_T014T STRING OPTIONS(description="Credit Control Area ID;T014T.KKBER where T014T.SPRAS = E"),
  KKBTX_T014T STRING OPTIONS(description="Credit Control Area Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);