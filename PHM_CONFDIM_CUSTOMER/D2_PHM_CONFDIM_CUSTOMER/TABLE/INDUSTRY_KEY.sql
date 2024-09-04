CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.INDUSTRY_KEY`
(
  BRSCH_T016T STRING OPTIONS(description="Industry key ID;T016T.BRSCH where T016T.SPRAS = E"),
  BRTXT_T016T STRING OPTIONS(description="Industry key Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);