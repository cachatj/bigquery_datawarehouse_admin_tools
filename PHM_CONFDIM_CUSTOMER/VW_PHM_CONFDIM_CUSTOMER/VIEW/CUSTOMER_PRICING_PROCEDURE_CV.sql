CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CUSTOMER_PRICING_PROCEDURE_CV`(
  CUSTOMER_PRICE_PRCDR_ID OPTIONS(description="Customer Pricing Procedure ID"),
  CUSTOMER_PRICE_PRCDR_DESC_TXT OPTIONS(description="Customer Pricing Procedure Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KALKS_TVKDT     AS     CUSTOMER_PRICE_PRCDR_ID
,VTEXT_TVKDT     AS     CUSTOMER_PRICE_PRCDR_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PRICING_PROCEDURE_CV`;