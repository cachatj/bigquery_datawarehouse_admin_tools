CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.INCOTERMS_CV`
(
  INCO1_TINCT STRING OPTIONS(description="Incoterms Part 1 ID;TINCT.INCO1 where TINCT.SPRAS = E"),
  BEZEI_TINCT STRING OPTIONS(description="Incoterms Part 1 Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY INCO1_TINCT;