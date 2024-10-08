CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.EAN_CATEGORY_CV`
(
  NUMTP_TNTPB STRING OPTIONS(description="EAN Category ID; TNTPB.NUMTP where TNTPB.SPRAS = E"),
  NTBEZ_TNTPB STRING OPTIONS(description="EAN Number category text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY NUMTP_TNTPB;