CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.PRICE_STICKER`
(
  YYPRCSTKR_YTSD_PRC_STKR_TX STRING OPTIONS(description="Price Sticker ID;YTSD_PRC_STKR_TX.YYPRCSTKR where YTSD_PRC_STKR_TX.SPRAS = E"),
  YYDES_YTSD_PRC_STKR_TX STRING OPTIONS(description="Price Sticker Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);