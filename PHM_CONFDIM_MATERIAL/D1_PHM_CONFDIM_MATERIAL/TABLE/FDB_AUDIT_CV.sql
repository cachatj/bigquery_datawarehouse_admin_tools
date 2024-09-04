CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_AUDIT_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  EVENTID STRING OPTIONS(description="Event ID"),
  EVENNAME STRING OPTIONS(description="Event Name"),
  SOURCESYSTEM STRING OPTIONS(description="Source System"),
  EVENTTYPE STRING OPTIONS(description="Event Type"),
  EVENTTIME STRING OPTIONS(description="Event Time"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, EVENTID;