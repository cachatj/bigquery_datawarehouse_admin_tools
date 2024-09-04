CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CLASS_OF_TRADE_LEVEL_ONE_CV`
(
  KUKLA_TKUKT STRING OPTIONS(description="Customer Classification ID;TKUKT.KUKLA where TKUKT.SPRAS = E"),
  VTEXT_TKUKT STRING OPTIONS(description="Customer Classification Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KUKLA_TKUKT;