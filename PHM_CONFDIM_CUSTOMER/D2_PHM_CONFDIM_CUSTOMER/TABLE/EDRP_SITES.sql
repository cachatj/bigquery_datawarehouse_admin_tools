CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.EDRP_SITES`
(
  YYEDRPSPD_YTSD_EDRPSPD_TXT STRING OPTIONS(description="EDRP Sites ID;YTSD_EDRPSPD_TXT.YYEDRPSPD where YTSD_EDRPSPD_TXT.SPRAS = E"),
  YYEDRPSPD_DES_YTSD_EDRPSPD_TXT STRING OPTIONS(description="EDRP Sites Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);