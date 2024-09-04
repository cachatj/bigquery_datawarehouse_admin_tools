CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.DEA_SUB_BASE_CODE_CV`
(
  YYSUBB_YTMM_SUBBASETEXT STRING OPTIONS(description="DEA Sub Base Code ID; YTMM_SUBBASETEXT.YYSUBB where YTMM_SUBBASETEXT.SPRAS = E"),
  YYDESC_YTMM_SUBBASETEXT STRING OPTIONS(description="DEA Sub Base Code Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY YYSUBB_YTMM_SUBBASETEXT;