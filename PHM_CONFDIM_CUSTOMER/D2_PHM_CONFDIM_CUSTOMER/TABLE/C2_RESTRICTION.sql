CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.C2_RESTRICTION`
(
  KATR8_TVK8T STRING OPTIONS(description="C2 Restriction ID;TVK8T.KATR8 where TVK8T.SPRAS = E"),
  VTEXT_TVK8T STRING OPTIONS(description="C2 Restriction Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);