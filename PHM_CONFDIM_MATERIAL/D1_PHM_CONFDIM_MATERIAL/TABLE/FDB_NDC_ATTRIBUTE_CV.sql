CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_NDC_ATTRIBUTE_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  NDC_ATTRIBUTE_TYPE_CD STRING OPTIONS(description="NDC Attribute Type Code"),
  NDC_ATTRIBUTE_TYPE_DSC STRING OPTIONS(description="NDC Attribute Type Code Description"),
  NDC_ATTRIBUTE_SN STRING OPTIONS(description="NDC Attribute Sequence Number"),
  NDC_ATTRIBUTE_VALUE STRING OPTIONS(description="NDC Attribute Value"),
  NDC_ATTRIBUTE_VALUE_DESC STRING OPTIONS(description="NDC Attribute Value Description"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, NDC_ATTRIBUTE_TYPE_CD, NDC_ATTRIBUTE_SN;