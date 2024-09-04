CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.BACKORDER_RESTRICTION`
(
  KATR6_TVK6T STRING OPTIONS(description="Backorder Restriction ID;TVK6T.KATR6 where TVK6T.SPRAS = E"),
  VTEXT_TVK6T STRING OPTIONS(description="Backorder Restriction Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);