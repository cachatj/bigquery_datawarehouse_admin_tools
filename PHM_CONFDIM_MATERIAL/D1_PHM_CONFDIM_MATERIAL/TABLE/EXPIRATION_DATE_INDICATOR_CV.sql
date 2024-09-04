CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.EXPIRATION_DATE_INDICATOR_CV`
(
  SLED_BBD_MARA STRING OPTIONS(description="Expiration Date Indicator ID"),
  EXP_DTE_IND_DESC_TXT STRING OPTIONS(description="Expiration Date Indicator Description Text; Case MARA.SLED_BBD, When B then EXPIRATION DATE, When E then SHELF LIFE EXPIRATION DATE, Else N/A"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY SLED_BBD_MARA;