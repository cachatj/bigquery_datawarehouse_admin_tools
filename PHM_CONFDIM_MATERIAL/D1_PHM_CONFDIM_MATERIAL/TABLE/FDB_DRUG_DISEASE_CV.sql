CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_DISEASE_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  DDXCN STRING OPTIONS(description="DDCM Drug-Disease Contraindications Code"),
  DDXCN_SN STRING OPTIONS(description="DDCM Sequence Number"),
  FDBDX STRING OPTIONS(description="First Databank Disease Code"),
  DDXCN_SL_DESC STRING OPTIONS(description="DDCM Severity Level"),
  DDXCN_REF STRING OPTIONS(description="DDCM Reference"),
  DXID STRING OPTIONS(description="FML Disease Identifier (Stable ID)"),
  DXID_DESC56 STRING OPTIONS(description="FML 56-character Description"),
  DXID_DESC100 STRING OPTIONS(description="FML 100-character Description"),
  DXID_STATUS STRING OPTIONS(description="FML DxID Status Code"),
  DXID_DISEASE_DURATION_CD STRING OPTIONS(description="FML Disease Duration Code"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, DDXCN, DDXCN_SN;