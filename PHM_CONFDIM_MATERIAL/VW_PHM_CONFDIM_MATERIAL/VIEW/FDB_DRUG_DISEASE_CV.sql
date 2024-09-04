CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.FDB_DRUG_DISEASE_CV`(
  NDC OPTIONS(description="National Drug Code"),
  DDXCN OPTIONS(description="DDCM Drug-Disease Contraindications Code"),
  DDXCN_SN OPTIONS(description="DDCM Sequence Number"),
  FDBDX OPTIONS(description="First Databank Disease Code"),
  DDXCN_SL_DESC OPTIONS(description="DDCM Severity Level"),
  DDXCN_REF OPTIONS(description="DDCM Reference"),
  DXID OPTIONS(description="FML Disease Identifier (Stable ID)"),
  DXID_DESC56 OPTIONS(description="FML 56-character Description"),
  DXID_DESC100 OPTIONS(description="FML 100-character Description"),
  DXID_STATUS OPTIONS(description="FML DxID Status Code"),
  DXID_DISEASE_DURATION_CD OPTIONS(description="FML Disease Duration Code"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
	NDC,
  DDXCN,
  DDXCN_SN,
  FDBDX,
  DDXCN_SL_DESC,
  DDXCN_REF,
  DXID,
  DXID_DESC56,
  DXID_DESC100,
  DXID_STATUS,
  DXID_DISEASE_DURATION_CD,
  ROW_ADD_STP,
	 ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_DISEASE_CV`;