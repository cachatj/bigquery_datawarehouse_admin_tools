CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.MULTIPLE_COPIES_PREFERENCE_CV`
(
  YYMULTCOP_KNVV INT64 OPTIONS(description="Multiple Copies Customer Preference Code;Distinct values of KNVV.YYMULTCOP"),
  MULTI_COPY_CUSTOMER_PREFER_DESC_TXT STRING OPTIONS(description="Multiple Copies Customer Preference Description Text;Case KNVV.YYMULTCOP when 1 then ONE COPY; when 2 then TWO COPIES; else NA"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYMULTCOP_KNVV;