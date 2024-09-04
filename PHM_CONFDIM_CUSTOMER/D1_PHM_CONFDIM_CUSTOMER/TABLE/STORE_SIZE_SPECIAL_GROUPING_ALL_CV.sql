CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.STORE_SIZE_SPECIAL_GROUPING_ALL_CV`
(
  KVGR4_TVV4T STRING OPTIONS(description="Store Size Special Grouping Allocation ID;TVV4T.KVGR4 where TVV4T.SPRAS = E"),
  BEZEI_TVV4T STRING OPTIONS(description="Store Size Special Grouping Allocation Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KVGR4_TVV4T;