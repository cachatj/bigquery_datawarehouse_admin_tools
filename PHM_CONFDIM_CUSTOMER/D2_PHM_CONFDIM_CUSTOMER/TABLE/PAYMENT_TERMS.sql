CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.PAYMENT_TERMS`
(
  ZTERM_TVZBT STRING OPTIONS(description="Payment Terms ID;TVZBT.ZTERM where TVZBT.SPRAS = E"),
  VTEXT_TVZBT STRING OPTIONS(description="Payment Terms Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);