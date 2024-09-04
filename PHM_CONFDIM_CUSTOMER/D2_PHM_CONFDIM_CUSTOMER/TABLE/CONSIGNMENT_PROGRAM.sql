CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CONSIGNMENT_PROGRAM`
(
  YYCONSIGN_YTSD_CONSIGN_TXT STRING OPTIONS(description="Consignment Program ID;YTSD_CONSIGN_TXT.YYCONSIGN where YTSD_CONSIGN_TXT.SPRAS = E"),
  YYCONSIGN_DES_YTSD_CONSIGN_TXT STRING OPTIONS(description="Consignment Program Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);