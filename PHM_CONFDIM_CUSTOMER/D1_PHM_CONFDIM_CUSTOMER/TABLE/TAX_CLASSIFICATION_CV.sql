CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TAX_CLASSIFICATION_CV`
(
  TAXKD_TSKDT STRING OPTIONS(description="Tax Classification ID;TSKDT.TAXKD where TSKDT.SPRAS = E and TSKDT.TATYP = UTXJ"),
  VTEXT_TSKDT STRING OPTIONS(description="Tax Classification Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY TAXKD_TSKDT;