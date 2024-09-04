CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.OTC_RESTRICTION`
(
  KATR7_TVK7T STRING OPTIONS(description="OTC Restriction ID;TVK7T.KATR7 where TVK7T.SPRAS = E"),
  VTEXT_TVK7T STRING OPTIONS(description="OTC Restriction Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);