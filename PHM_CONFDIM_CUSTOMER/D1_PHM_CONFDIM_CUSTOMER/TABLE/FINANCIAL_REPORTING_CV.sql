CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.FINANCIAL_REPORTING_CV`
(
  KVGR5_TVV5T STRING OPTIONS(description="Financial Reporting ID;TVV5T.KVGR5 where TVV5T.SPRAS = E"),
  BEZEI_TVV5T STRING OPTIONS(description="Financial Reporting Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KVGR5_TVV5T;