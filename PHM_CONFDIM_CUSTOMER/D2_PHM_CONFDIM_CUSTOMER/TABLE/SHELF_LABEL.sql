CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SHELF_LABEL`
(
  YYSHELFLAB_YTSD_SHELFLABTXT STRING OPTIONS(description="Shelf Label ID;YTSD_SHELFLABTXT.YYSHELFLAB where YTSD_SHELFLABTXT.SPRAS = E"),
  YYSHELFLAB_DES_YTSD_SHELFLABTXT STRING OPTIONS(description="Shelf Label Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);