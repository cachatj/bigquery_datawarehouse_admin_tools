CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.FUEL_SURCHARGE`
(
  YYFUELCHG_YTSD_FUELCHG_TXT STRING OPTIONS(description="Fuel Surcharge ID;YTSD_FUELCHG_TXT.YYFUELCHG where YTSD_FUELCHG_TXT.SPRAS = E"),
  YYFUELCHG_DES_YTSD_FUELCHG_TXT STRING OPTIONS(description="Fuel Surcharge Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);