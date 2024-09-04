CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_ACCOUNT_GROUP_CV`
(
  KTOKD_T077X STRING OPTIONS(description="Customer Account Group ID;T077X.KTOKD where T077X.SPRAS = E"),
  TXT30_T077X STRING OPTIONS(description="Customer Account Group Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KTOKD_T077X;