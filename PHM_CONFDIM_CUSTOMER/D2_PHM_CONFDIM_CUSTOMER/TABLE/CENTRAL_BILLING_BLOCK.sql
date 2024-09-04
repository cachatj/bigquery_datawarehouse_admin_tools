CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CENTRAL_BILLING_BLOCK`
(
  FAKSP_TVFST STRING OPTIONS(description="Central Billing Block ID;TVFST.FAKSP where TVFST.SPRAS = E"),
  VTEXT_TVFST STRING OPTIONS(description="Central Billing Block Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);