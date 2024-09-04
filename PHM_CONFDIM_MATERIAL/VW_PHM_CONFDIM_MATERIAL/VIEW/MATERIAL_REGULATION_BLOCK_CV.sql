CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_REGULATION_BLOCK_CV`(
  REGULATION_ID OPTIONS(description="Regulation ID"),
  PLANT_ID OPTIONS(description="Plant ID"),
  MTRL_ID OPTIONS(description="Material Number"),
  SUPPLIER_ID OPTIONS(description="Supplier Number"),
  BLCK_TYPE_ID OPTIONS(description="Blocking Type ID"),
  BLCK_TYPE_DESC_TXT OPTIONS(description="Blocking Type Description Text"),
  CHK_LIC_TABLE_IND_ID OPTIONS(description="Check License Table Indicator ID"),
  CHK_LIC_TABLE_IND_DESC_TXT OPTIONS(description="Check License Table Indicator Description Text"),
  LIC_TYPE_ID OPTIONS(description="License Type ID"),
  LIC_TYPE_DESCRCIPTION_TXT OPTIONS(description="License Type Descrciption Text"),
  REC_STAT_ID OPTIONS(description="Record Status ID"),
  REC_STAT_DESC_TXT OPTIONS(description="Record Status Description Text"),
  THREE_PL_IND_FLG OPTIONS(description="3PL Indicator Flag"),
  CREATE_BY_USER_ID OPTIONS(description="Created By User ID"),
  CREATE_ON_DTE OPTIONS(description="Created On Date"),
  CHG_BY_USER_ID OPTIONS(description="Changed By User ID"),
  CHG_ON_DTE OPTIONS(description="Changed On Date"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
OPTIONS(
  description=""
)
AS SELECT
	YREGULATION_ID_YTMM_REGUL_BLOCK			AS	REGULATION_ID,
	WERKS_YTMM_REGUL_BLOCK         			AS	PLANT_ID,
	MATNR_YTMM_REGUL_BLOCK         			AS	MTRL_ID,
	LIFNR_YTMM_REGUL_BLOCK         			AS	SUPPLIER_ID,
	YBLOCK_TYPE_YTMM_REGUL_BLOCK            AS	BLCK_TYPE_ID,
	CALC_BLCK_TYPE_DESC_TXT                 AS	BLCK_TYPE_DESC_TXT,
	YCHECK_LIC_YTMM_REGUL_BLOCK             AS	CHK_LIC_TABLE_IND_ID,
	CALC_CHK_LIC_TABLE_IND_DESC_TXT         AS	CHK_LIC_TABLE_IND_DESC_TXT,
	YP_LICENSE_TYPE_YTMM_REGUL_BLOCK        AS	LIC_TYPE_ID,
	YP_LICENSE_DESC_YTMM_LICENSE_TYT        AS	LIC_TYPE_DESCRCIPTION_TXT,
	YRECORDSTAT_YTMM_REGUL_BLOCK            AS	REC_STAT_ID,
	CALC_REC_STAT_DESC_TXT                  AS	REC_STAT_DESC_TXT,
	Y3PLFLG_YTMM_REGUL_BLOCK                AS	THREE_PL_IND_FLG,
	ERNAM_YTMM_REGUL_BLOCK                  AS	CREATE_BY_USER_ID,
	ERDAT_YTMM_REGUL_BLOCK                  AS	CREATE_ON_DTE,
	AENAM_YTMM_REGUL_BLOCK                  AS	CHG_BY_USER_ID,
	AEDAT_YTMM_REGUL_BLOCK                  AS	CHG_ON_DTE,
	ROW_ADD_STP                             AS	ROW_ADD_STP,
	ROW_ADD_USER_ID                         AS	ROW_ADD_USER_ID,
	ROW_UPDATE_STP                          AS	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID                      AS	ROW_UPDATE_USER_ID
FROM
   `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_REGULATION_BLOCK_CV`;