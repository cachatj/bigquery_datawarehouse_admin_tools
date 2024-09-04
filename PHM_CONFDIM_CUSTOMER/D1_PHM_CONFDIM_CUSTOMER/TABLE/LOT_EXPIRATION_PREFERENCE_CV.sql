CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.LOT_EXPIRATION_PREFERENCE_CV`
(
  YYLOTEXP_YTSD_LOT_EXP_TXT STRING OPTIONS(description="Lot Expiration Preference ID;YTSD_LOT_EXP_TXT.YYLOTEXP where YTSD_LOT_EXP_TXT.SPRAS = E"),
  YYDES_YTSD_LOT_EXP_TXT STRING OPTIONS(description="Lot Expiration Preference Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYLOTEXP_YTSD_LOT_EXP_TXT;