CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_IMPRINTS_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  IPTUNIQID STRING OPTIONS(description="Imprint Unique Drug ID"),
  IPTDFID STRING OPTIONS(description="Imprint Dosage Form ID"),
  IPTMFGID STRING OPTIONS(description="Imprint Manufacturer ID"),
  IPTMFGNAME STRING OPTIONS(description="Imprint Manufacturer Name"),
  IPTDFDESC STRING OPTIONS(description="Imprint Dosage Form Description"),
  IPTPROPID STRING OPTIONS(description="Imprint Property ID"),
  IPTLBLID STRING OPTIONS(description="Imprint Label ID"),
  IPTSIDE1 STRING OPTIONS(description="Imprint Side 1"),
  IPTSIDE2 STRING OPTIONS(description="Imprint Side 2"),
  IPTTEXTID STRING OPTIONS(description="Imprint Text ID"),
  IPTLINENO STRING OPTIONS(description="Imprint Line Number"),
  IPTTEXT STRING OPTIONS(description="Imprint Text"),
  CHG_FLAG STRING OPTIONS(description="RNDC14_NDC_MSTR Change Flag"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, IPTUNIQID, IPTLINENO;