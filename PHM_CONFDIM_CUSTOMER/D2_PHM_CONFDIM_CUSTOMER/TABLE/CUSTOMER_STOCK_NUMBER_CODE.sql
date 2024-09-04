CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CUSTOMER_STOCK_NUMBER_CODE`
(
  YYCOMMODITY_KNVV STRING OPTIONS(description="Customer Stock Number ID;Distinct values of KNVV.YYCOMMODITY"),
  CUSTOMER_STOCK_NUM_DESC_TXT STRING OPTIONS(description="Customer Stock Number Description Text;Case KNVV.YYCOMMODITY when 01 then CSN REPLACEMENT; when 02 then CSN ADDITION; else NA"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);