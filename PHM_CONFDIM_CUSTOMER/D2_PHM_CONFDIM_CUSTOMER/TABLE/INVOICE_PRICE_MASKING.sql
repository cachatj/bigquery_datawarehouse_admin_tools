CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.INVOICE_PRICE_MASKING`
(
  YYPRICEMASKINV_YTSD_PREMSKINVTX STRING OPTIONS(description="Invoice Price Masking ID;YTSD_PREMSKINVTX.YYPRICEMASKINV where YTSD_PREMSKINVTX.SPRAS = E"),
  YYPRICEMASKINV_DES_YTSD_PREMSKINVTX STRING OPTIONS(description="Invoice Price Masking Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);