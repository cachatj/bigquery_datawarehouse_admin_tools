CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SPD_VITALSOURCE_MEMBERSHIP_TYPE`
(
  YYTYPE_YTSD_TYPE_TXT STRING OPTIONS(description="SPD Vitalsource Membership Type ID;YTSD_TYPE_TXT.YYTYPE where YTSD_TYPE_TXT.SPRAS = E"),
  YYTYPE_DES_YTSD_TYPE_TXT STRING OPTIONS(description="SPD Vitalsource Membership Type Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);