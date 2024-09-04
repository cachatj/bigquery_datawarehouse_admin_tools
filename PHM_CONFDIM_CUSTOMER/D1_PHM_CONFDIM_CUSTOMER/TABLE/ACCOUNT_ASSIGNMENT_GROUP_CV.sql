CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.ACCOUNT_ASSIGNMENT_GROUP_CV`
(
  KTGRD_TVKTT STRING OPTIONS(description="Account Assignment Group ID;TVKTT.KTGRD where TVKTT.SPRAS = E"),
  VTEXT_TVKTT STRING OPTIONS(description="Account Assignment Group Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KTGRD_TVKTT;