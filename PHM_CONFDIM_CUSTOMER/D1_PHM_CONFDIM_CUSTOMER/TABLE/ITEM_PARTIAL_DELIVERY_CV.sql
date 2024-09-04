CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.ITEM_PARTIAL_DELIVERY_CV`
(
  KZTLF_KNVV STRING OPTIONS(description="Item Partial Deliveries Allowed ID;Distinct value of KNVV.KZTLF"),
  ITEM_PRTL_DELIVERIES_ALW_DESC_TXT STRING OPTIONS(description="Item Partial Deliveries Allowed Description Text;Case KNVV.KZTLF when Blank then PARTIAL DELIVERIES ALLOWED, when A then CREATE A DELIVERY WITH QTY GREATER THAN ZERO, when B then CREATE ONLY ONE DELIVERY (ALSO WITH QUANTITY = 0), when C then ONLY COMPLETE DELIVERY ALLOWED, when D then NO LIMIT TO SUBSEQUENT DELIVERIES, Else NA)"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KZTLF_KNVV;