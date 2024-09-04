CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MEDISPAN_INGREDIENT_CV`(
  NDC_UPC_HRI_ID OPTIONS(description=" NDC-UPC-HRI "),
  INGRED_GEN_ID OPTIONS(description=" Ingredient Generic ID Number "),
  INGRED_STRGTH_ID OPTIONS(description=" Ingredient Strength "),
  INGRED_STRGTH_UOM_ID OPTIONS(description=" Ingredient Strength Unit of Measure "),
  GEN_INGRED_NAM OPTIONS(description=" Generic Ingredient Name "),
  ACTIVE_INGRED_FLG OPTIONS(description=" Active/Inactive Ingredient Flag"),
  MDFY_ID OPTIONS(description=" Modifier Code "),
  MODIFIER_DESCRIPTION OPTIONS(description=" Modifier Description "),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
NDC_UPC_HRI_ID
,INGRED_GEN_ID
,INGRED_STRGTH_ID
,INGRED_STRGTH_UOM_ID
,GEN_INGRED_NAM
,ACTIVE_INGRED_FLG
,MDFY_ID
,MODIFIER_DESCRIPTION
,ROW_ADD_STP
,ROW_ADD_USER_ID
,ROW_UPDATE_STP
,ROW_UPDATE_USER_ID

FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_INGREDIENT_CV`;