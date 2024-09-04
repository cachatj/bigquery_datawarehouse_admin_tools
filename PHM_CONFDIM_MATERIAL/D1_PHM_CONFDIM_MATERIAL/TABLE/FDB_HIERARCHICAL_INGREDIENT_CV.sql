CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV`
(
  NDC STRING OPTIONS(description="National Drug Code"),
  HIC_SEQN STRING OPTIONS(description="Hierarchical Ingredient Code Sequence Number (Stable ID)"),
  HIC_REL_NO STRING OPTIONS(description="Hierarchical Ingredient Code Relative Number"),
  HIC STRING OPTIONS(description="Hierarchical Ingredient Code"),
  HIC_DESC STRING OPTIONS(description="Hierarchical Ingredient Code Description"),
  HIC_ROOT STRING OPTIONS(description="Hierarchical Ingredient Parent HIC4 Sequence Number"),
  HIC_POTENTIALLY_INACTV_IND STRING OPTIONS(description="Hierarchical Ingredient Code Sequence Number Potentially Inactive Indicator"),
  ING_STATUS_CD STRING OPTIONS(description="Ingredient Status Code"),
  CAS9_TBL STRING OPTIONS(description="Chemical Abstracts Service Registry Number"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC, HIC_SEQN;