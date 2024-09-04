CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CLASS_OF_TRADE_LEVEL_THREE_CV`
(
  KVGR1_TVV1T STRING OPTIONS(description="Class Of Trade Level 3 ID;TVV1T.KVGR1 where TVV1T.SPRAS = E"),
  BEZEI_TVV1T STRING OPTIONS(description="Class Of Trade Level 3 Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KVGR1_TVV1T;