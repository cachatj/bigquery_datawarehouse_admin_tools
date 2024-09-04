CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_INGREDIENT_CV`
(
  NDC_UPC_HRI_ID STRING OPTIONS(description=" NDC-UPC-HRI "),
  INGRED_GEN_ID STRING OPTIONS(description=" Ingredient Generic ID Number "),
  INGRED_STRGTH_ID STRING OPTIONS(description=" Ingredient Strength "),
  INGRED_STRGTH_UOM_ID STRING OPTIONS(description=" Ingredient Strength Unit of Measure "),
  GEN_INGRED_NAM STRING OPTIONS(description=" Generic Ingredient Name "),
  ACTIVE_INGRED_FLG STRING OPTIONS(description=" Active/Inactive Ingredient Flag"),
  MDFY_ID STRING OPTIONS(description=" Modifier Code "),
  MODIFIER_DESCRIPTION STRING OPTIONS(description=" Modifier Description "),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATE(ROW_UPDATE_STP)
CLUSTER BY NDC_UPC_HRI_ID, INGRED_GEN_ID;