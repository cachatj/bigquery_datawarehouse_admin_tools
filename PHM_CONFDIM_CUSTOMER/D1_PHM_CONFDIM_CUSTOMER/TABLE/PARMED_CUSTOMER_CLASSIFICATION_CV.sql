CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PARMED_CUSTOMER_CLASSIFICATION_CV`
(
  KVGR2_TVV2T STRING OPTIONS(description="Parmed Customer Classification ID;TVV2T.KVGR2 where TVV2T.SPRAS = E"),
  BEZEI_TVV2T STRING OPTIONS(description="Parmed Customer Classification Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KVGR2_TVV2T;