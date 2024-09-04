CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.FDB_AUDIT_CV`(
  NDC OPTIONS(description="National Drug Code"),
  EVENTID OPTIONS(description="Event ID"),
  EVENNAME OPTIONS(description="Event Name"),
  SOURCESYSTEM OPTIONS(description="Source System"),
  EVENTTYPE OPTIONS(description="Event Type"),
  EVENTTIME OPTIONS(description="Event Time"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 NDC,
  EVENTID,
  EVENNAME,
  SOURCESYSTEM,
  EVENTTYPE,
  EVENTTIME,
  ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID

FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_AUDIT_CV`;