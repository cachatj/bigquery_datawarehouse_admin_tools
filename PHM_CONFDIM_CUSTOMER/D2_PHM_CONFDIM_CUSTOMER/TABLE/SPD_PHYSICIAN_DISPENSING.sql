CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SPD_PHYSICIAN_DISPENSING`
(
  YYPHYDISP_YTSD_PHYDIS_TEXT STRING OPTIONS(description="SPD Physician Dispensing ID;YTSD_PHYDIS_TEXT.YYPHYDISP where YTSD_PHYDIS_TEXT.SPRAS = E"),
  YYPHYDISP_DES_YTSD_PHYDIS_TEXT STRING OPTIONS(description="SPD Physician Dispensing Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);