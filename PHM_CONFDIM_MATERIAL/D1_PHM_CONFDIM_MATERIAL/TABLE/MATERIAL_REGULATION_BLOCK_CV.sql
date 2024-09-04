CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_REGULATION_BLOCK_CV`
(
  YREGULATION_ID_YTMM_REGUL_BLOCK STRING NOT NULL OPTIONS(description="System-generated ID for each regulation"),
  WERKS_YTMM_REGUL_BLOCK STRING NOT NULL OPTIONS(description="Plant ID"),
  MATNR_YTMM_REGUL_BLOCK STRING NOT NULL OPTIONS(description="Material Number"),
  LIFNR_YTMM_REGUL_BLOCK STRING NOT NULL OPTIONS(description="Supplier Number"),
  YBLOCK_TYPE_YTMM_REGUL_BLOCK STRING NOT NULL OPTIONS(description="Blocking Type ID"),
  CALC_BLCK_TYPE_DESC_TXT STRING OPTIONS(description="Blocking Type Description Text; Case YTMM_REGUL_BLOCK.YBLOCK_TYPE When I Then Invalid When P Then Purchase When M Then Message When R Then Receiving When O Then Override When Blank Then Valid License Else NOT_AVAILABLE End"),
  YCHECK_LIC_YTMM_REGUL_BLOCK STRING OPTIONS(description="Check License Table Indicator ID"),
  CALC_CHK_LIC_TABLE_IND_DESC_TXT STRING OPTIONS(description="Check License Table Indicator Description Text; Case YTMM_REGUL_BLOCK.YCHECK_LIC When Y Then Yes When N Then No When S Then Rule based on Ship From Else NOT_AVAILABLE End"),
  YP_LICENSE_TYPE_YTMM_REGUL_BLOCK STRING OPTIONS(description="License Type ID"),
  YP_LICENSE_DESC_YTMM_LICENSE_TYT STRING OPTIONS(description="License Type Descrciption Text; YTMM_LICENSE_TYT.YP_LICENSE_DESC where YTMM_LICENSE_TYT.YP_LICENSE_TYPE = YTMM_REGUL_BLOCK.YP_LICENSE_TYPE and YTMM_LICENSE_TYT.SPRAS = E"),
  YRECORDSTAT_YTMM_REGUL_BLOCK STRING OPTIONS(description="Record Status ID"),
  CALC_REC_STAT_DESC_TXT STRING OPTIONS(description="Record Status Description Text; Case YTMM_REGUL_BLOCK.YRECORDSTAT When A Then Approved When P Then Pending When D Then Deleted Else NOT_AVAILABLE End"),
  Y3PLFLG_YTMM_REGUL_BLOCK STRING OPTIONS(description="3PL Indicator Flag"),
  ERNAM_YTMM_REGUL_BLOCK STRING OPTIONS(description="Created By User ID"),
  ERDAT_YTMM_REGUL_BLOCK DATE OPTIONS(description="Created On Date"),
  AENAM_YTMM_REGUL_BLOCK STRING OPTIONS(description="Changed By User ID"),
  AEDAT_YTMM_REGUL_BLOCK DATE OPTIONS(description="Changed On Date"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY WERKS_YTMM_REGUL_BLOCK, MATNR_YTMM_REGUL_BLOCK, LIFNR_YTMM_REGUL_BLOCK, YBLOCK_TYPE_YTMM_REGUL_BLOCK
OPTIONS(
  description=""
);