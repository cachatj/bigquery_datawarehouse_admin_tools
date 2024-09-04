CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CREDIT_MEMO_PREFERENCES`
(
  YYCRMEMO_YTSD_CRMEMO_TEXT STRING OPTIONS(description="Credit Memo Preferences ID;YTSD_CRMEMO_TEXT.YYCRMEMO where YTSD_CRMEMO_TEXT.SPRAS = E"),
  YYCRMEMO_DES_YTSD_CRMEMO_TEXT STRING OPTIONS(description="Credit Memo Preferences Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);