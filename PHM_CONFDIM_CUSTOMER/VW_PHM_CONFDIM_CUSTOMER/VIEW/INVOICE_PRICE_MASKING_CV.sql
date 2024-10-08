CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.INVOICE_PRICE_MASKING_CV`(
  INVOICE_PRICE_MASKING_ID OPTIONS(description="Invoice Price Masking ID"),
  INVOICE_PRICE_MASKING_DESC_TXT OPTIONS(description="Invoice Price Masking Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYPRICEMASKINV_YTSD_PREMSKINVTX     AS     INVOICE_PRICE_MASKING_ID
,YYPRICEMASKINV_DES_YTSD_PREMSKINVTX     AS     INVOICE_PRICE_MASKING_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.INVOICE_PRICE_MASKING_CV`;