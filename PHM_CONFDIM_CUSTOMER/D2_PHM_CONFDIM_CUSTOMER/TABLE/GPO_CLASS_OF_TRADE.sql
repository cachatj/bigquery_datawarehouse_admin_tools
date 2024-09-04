CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.GPO_CLASS_OF_TRADE`
(
  KVGR3_TVV3T STRING OPTIONS(description="GPO Class Of Trade ID;TVV3T.KVGR3 where TVV3T.SPRAS = E"),
  BEZEI_TVV3T STRING OPTIONS(description="GPO Class Of Trade Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);