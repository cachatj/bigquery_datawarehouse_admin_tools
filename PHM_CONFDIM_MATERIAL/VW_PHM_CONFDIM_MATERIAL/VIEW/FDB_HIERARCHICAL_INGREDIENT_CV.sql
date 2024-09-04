CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV`(
  NDC OPTIONS(description="National Drug Code"),
  HIC_SEQN OPTIONS(description="Hierarchical Ingredient Code Sequence Number (Stable ID)"),
  HIC_REL_NO OPTIONS(description="Hierarchical Ingredient Code Relative Number"),
  HIC OPTIONS(description="Hierarchical Ingredient Code"),
  HIC_DESC OPTIONS(description="Hierarchical Ingredient Code Description"),
  HIC_ROOT OPTIONS(description="Hierarchical Ingredient Parent HIC4 Sequence Number"),
  HIC_POTENTIALLY_INACTV_IND OPTIONS(description="Hierarchical Ingredient Code Sequence Number Potentially Inactive Indicator"),
  ING_STATUS_CD OPTIONS(description="Ingredient Status Code"),
  CAS9_TBL OPTIONS(description="Chemical Abstracts Service Registry Number"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
NDC,
HIC_SEQN,
HIC_REL_NO,
  HIC,
  HIC_DESC,
  HIC_ROOT,
  HIC_POTENTIALLY_INACTV_IND,
  ING_STATUS_CD,
  CAS9_TBL,
 ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_HIERARCHICAL_INGREDIENT_CV`;