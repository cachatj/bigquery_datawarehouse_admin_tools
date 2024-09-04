CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CUSTOMER_STOREFRONT`
(
  YYSTOREFRONT_YTSD_STORFRNTTXT STRING OPTIONS(description="Customer Storefront ID;YTSD_STORFRNTTXT.YYSTOREFRONT where YTSD_STORFRNTTXT.SPRAS = E"),
  YYSTOREFRONT_DES_YTSD_STORFRNTTXT STRING OPTIONS(description="Customer Storefront Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);