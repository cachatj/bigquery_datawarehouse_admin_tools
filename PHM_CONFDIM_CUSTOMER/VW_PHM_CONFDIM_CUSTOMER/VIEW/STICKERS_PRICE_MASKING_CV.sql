CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.STICKERS_PRICE_MASKING_CV`(
  STICKERS_PRICE_MASKING_ID OPTIONS(description="Stickers Price Masking ID"),
  STICKERS_PRICE_MASKING_DESC_TXT OPTIONS(description="Stickers Price Masking Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYSTMASKPRE_YTSD_STMASKPRETX     AS     STICKERS_PRICE_MASKING_ID
,YYSTMASKPRE_DES_YTSD_STMASKPRETX     AS     STICKERS_PRICE_MASKING_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.STICKERS_PRICE_MASKING_CV`;