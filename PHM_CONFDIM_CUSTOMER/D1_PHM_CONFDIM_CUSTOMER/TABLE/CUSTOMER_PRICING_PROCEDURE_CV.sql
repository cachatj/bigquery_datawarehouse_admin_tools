CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_PRICING_PROCEDURE_CV`
(
  KALKS_TVKDT STRING OPTIONS(description="Customer Pricing Procedure ID;TVKDT.KALKS where TVKDT.SPRAS = E"),
  VTEXT_TVKDT STRING OPTIONS(description="Customer Pricing Procedure Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KALKS_TVKDT;