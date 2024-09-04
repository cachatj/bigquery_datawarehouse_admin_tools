CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SHIPPING_CONDITIONS`
(
  VSBED_TVSBT STRING OPTIONS(description="Shipping Conditions ID;TVSBT.VSBED where TVSBT.SPRAS = E"),
  VTEXT_TVSBT STRING OPTIONS(description="Shipping Conditions Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);