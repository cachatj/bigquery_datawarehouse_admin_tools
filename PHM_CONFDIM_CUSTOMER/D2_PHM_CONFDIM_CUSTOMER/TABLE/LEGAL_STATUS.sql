CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.LEGAL_STATUS`
(
  GFORM_TVGFT STRING OPTIONS(description="Legal status ID;TVGFT.GFORM where TVGFT.SPRAS = E"),
  VTEXT_TVGFT STRING OPTIONS(description="Legal status Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);