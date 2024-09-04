CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK`
(
  LIFSP_TVLS STRING OPTIONS(description="Central Delivery Block ID"),
  VTEXT_TVLST STRING OPTIONS(description="Central Delivery Block Description Text;TVLST.VTEXT where TVLST.LIFSP = TVLS.LIFSP and TVLST.SPRAS = E"),
  SPELF_TVLS STRING OPTIONS(description="Delivery Block ID"),
  SPEKO_TVLS STRING OPTIONS(description="Picking Block ID"),
  SPEWA_TVLS STRING OPTIONS(description="Goods Issue Block ID"),
  SPEFT_TVLS STRING OPTIONS(description="Block Production ID"),
  SPEBE_TVLS STRING OPTIONS(description="Confirmation Block ID"),
  SPEAU_TVLS STRING OPTIONS(description="Sales Order Block ID"),
  SPEDR_TVLS STRING OPTIONS(description="Printing Block ID"),
  SPEVI_TVLS STRING OPTIONS(description="Delivery Due List Block ID"),
  MBDIF_TVLS INT64 OPTIONS(description="Material Provision Deferment Period Day Quantity"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);