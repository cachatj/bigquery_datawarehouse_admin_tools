CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.PRIMARY_SECONDARY_CODE`
(
  YYPRISEC_YTSD_PRISEC_TEXT STRING OPTIONS(description="Primary Secondary ID;YTSD_PRISEC_TEXT.YYPRISEC where YTSD_PRISEC_TEXT.SPRAS = E"),
  YYPRISEC_DES_YTSD_PRISEC_TEXT STRING OPTIONS(description="Primary Secondary Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);