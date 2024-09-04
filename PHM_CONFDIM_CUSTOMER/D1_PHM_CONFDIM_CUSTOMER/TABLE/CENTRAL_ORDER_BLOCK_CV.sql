CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CENTRAL_ORDER_BLOCK_CV`
(
  AUFSP_TVAST STRING OPTIONS(description="Central Order Block ID;TVAST.AUFSP where TVAST.SPRAS = E"),
  VTEXT_TVAST STRING OPTIONS(description="Central Order Block Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY AUFSP_TVAST;