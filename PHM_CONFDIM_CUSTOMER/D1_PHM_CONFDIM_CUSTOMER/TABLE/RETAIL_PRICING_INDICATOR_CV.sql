CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.RETAIL_PRICING_INDICATOR_CV`
(
  YYRETAIL_KNVV STRING OPTIONS(description="Retail Pricing Indicator ID;Distinct values of KNVV.YYRETAIL"),
  RETAIL_PRICE_IND_DESC_TXT STRING OPTIONS(description="Retail Pricing Indicator Description Text;Case KNVV.YYRETAIL when 01 then RETAIL NO; PROMO NO, when 02 then RETAIL YES; PROMO NO, when 03 then RETAIL YES; PROMO YES"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYRETAIL_KNVV;