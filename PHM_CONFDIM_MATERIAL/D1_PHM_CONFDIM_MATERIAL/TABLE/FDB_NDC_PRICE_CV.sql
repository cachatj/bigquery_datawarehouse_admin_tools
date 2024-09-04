CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_NDC_PRICE_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  PRICE_TYPE STRING OPTIONS(description="Price Type"),
  PRICE_TYPE_DESCRIPTION STRING OPTIONS(description="Price Type Description"),
  PRICE STRING OPTIONS(description="Price"),
  PRICE_EFFECTIVE_DT STRING OPTIONS(description="Price Effective Date"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, PRICE_TYPE;