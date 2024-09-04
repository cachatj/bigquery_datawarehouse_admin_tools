CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.STICKERS_PRICE_MASKING`
(
  YYSTMASKPRE_YTSD_STMASKPRETX STRING OPTIONS(description="Stickers Price Masking ID;YTSD_STMASKPRETX.YYSTMASKPRE where YTSD_STMASKPRETX.SPRAS = E"),
  YYSTMASKPRE_DES_YTSD_STMASKPRETX STRING OPTIONS(description="Stickers Price Masking Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);