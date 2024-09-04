CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION`
(
  YYBOPRE1_YTSD_BOPRE1_TEXT STRING OPTIONS(description="Primary Backorder Selection ID;YTSD_BOPRE1_TEXT.YYBOPRE1 where YTSD_BOPRE1_TEXT.SPRAS = E"),
  YYBOPRE1_DES_YTSD_BOPRE1_TEXT STRING OPTIONS(description="Primary Backorder Selection Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);