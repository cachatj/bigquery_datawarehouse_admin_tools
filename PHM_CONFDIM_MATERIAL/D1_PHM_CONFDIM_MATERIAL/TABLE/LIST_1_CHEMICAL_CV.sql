CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LIST_1_CHEMICAL_CV`
(
  YYLSTO_YTMM_LIST1_TEXT STRING OPTIONS(description="List 1 Chemical ID; YTMM_LIST1_TEXT.YYLSTO where YTMM_LIST1_TEXT.SPRAS = E"),
  YYDESC_YTMM_LIST1_TEXT STRING OPTIONS(description="List 1 Chemical Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY YYLSTO_YTMM_LIST1_TEXT;