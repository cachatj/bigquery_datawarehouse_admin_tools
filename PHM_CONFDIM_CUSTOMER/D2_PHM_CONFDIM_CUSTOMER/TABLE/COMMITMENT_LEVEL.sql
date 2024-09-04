CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.COMMITMENT_LEVEL`
(
  YYCOMMITMNT_YTSD_COMMIT_TXT STRING OPTIONS(description="Commitment Level ID;YTSD_COMMIT_TXT.YYCOMMITMNT where YTSD_COMMIT_TXT.SPRAS = E"),
  YYCOMMITMNT_DES_YTSD_COMMIT_TXT STRING OPTIONS(description="Commitment Level Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);