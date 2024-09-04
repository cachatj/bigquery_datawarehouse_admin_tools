CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_EQUIVALENCE_CV`(
  MTRL_NUM OPTIONS(description="Material Number"),
  MTRL_DESC_TXT OPTIONS(description="Material Description Text"),
  EQV_MTRL_NUM OPTIONS(description="Equivalent Material Number"),
  EQV_MTRL_DESC_TXT OPTIONS(description="Equivalent Material Description Text"),
  EQV_TYPE_CDE OPTIONS(description="Equivalence Type Code"),
  EQV_TYPE_DESC_TXT OPTIONS(description="Equivalence Type Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Updated User ID")
)
OPTIONS(
  description=""
)
AS SELECT
	MATNR_YTMM_EQV_ITEM   		AS MTRL_NUM,
	MAKTX_MAKT            		AS MTRL_DESC_TXT,
	YYEQVNUM_YTMM_EQV_ITEM		AS EQV_MTRL_NUM,
	MAKTX_MAKT_EQV        		AS EQV_MTRL_DESC_TXT,
	YYEQVTYP_YTMM_EQV_ITEM		AS EQV_TYPE_CDE,
	YYDESC_YTMM_EQVTYP_T  		AS EQV_TYPE_DESC_TXT,
	ROW_ADD_STP           		AS ROW_ADD_STP,
	ROW_ADD_USER_ID       		AS ROW_ADD_USER_ID,
	ROW_UPDATE_STP        		AS ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID    		AS ROW_UPDATE_USER_ID
FROM
   PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_EQUIVALENCE_CV;