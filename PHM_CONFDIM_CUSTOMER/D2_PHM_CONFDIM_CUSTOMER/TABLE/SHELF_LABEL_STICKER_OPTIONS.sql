CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_STICKER_OPTIONS`
(
  YYSTKSLOPT_YTSD_STKSLOPTTXT STRING OPTIONS(description="Shelf Label Sticker Options ID;YTSD_STKSLOPTTXT.YYSTKSLOPT where YTSD_STKSLOPTTXT.SPRAS = E"),
  YYSTKSLOPT_DES_YTSD_STKSLOPTTXT STRING OPTIONS(description="Shelf Label Sticker Options Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);