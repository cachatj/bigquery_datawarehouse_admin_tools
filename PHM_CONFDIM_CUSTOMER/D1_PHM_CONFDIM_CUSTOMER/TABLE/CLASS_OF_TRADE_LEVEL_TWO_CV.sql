CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CLASS_OF_TRADE_LEVEL_TWO_CV`
(
  KDGRP_T151T STRING OPTIONS(description="Class Of Trade Level 2 ID;T151T.KDGRP where T151T.SPRAS = E"),
  KTEXT_T151T STRING OPTIONS(description="Class Of Trade Level 2 Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY KDGRP_T151T;