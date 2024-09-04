CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.IMS_CODE`
(
  YYIMS_YTSD_IMS_CODETXT STRING OPTIONS(description="IMS ID;YTSD_IMS_CODETXT.YYIMS where YTSD_IMS_CODETXT.SPRAS = E"),
  YYIMS_DES_YTSD_IMS_CODETXT STRING OPTIONS(description="IMS Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);