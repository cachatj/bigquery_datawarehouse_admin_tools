CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.RETURNS_GROUP`
(
  YYRETGRP_YTSD_RETGRP_TEXT STRING OPTIONS(description="Returns Group ID;YTSD_RETGRP_TEXT.YYRETGRP where YTSD_RETGRP_TEXT.SPRAS = E"),
  YYRETGRP_DES_YTSD_RETGRP_TEXT STRING OPTIONS(description="Returns Group Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);