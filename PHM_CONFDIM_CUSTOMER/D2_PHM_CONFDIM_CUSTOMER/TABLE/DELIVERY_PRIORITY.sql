CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.DELIVERY_PRIORITY`
(
  LPRIO_TPRIT INT64 OPTIONS(description="Delivery Priority Code;TPRIT.LPRIO where TPRIT.SPRAS = E"),
  BEZEI_TPRIT STRING OPTIONS(description="Delivery Priority Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);