CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PARTNER_FUNCTION_CODE_CV`
(
  PARVW_TPART STRING OPTIONS(description="Partner Function ID;TPART.PARVW where TPART.SPRAS = E"),
  VTEXT_TPART STRING OPTIONS(description="Partner Function Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY PARVW_TPART;