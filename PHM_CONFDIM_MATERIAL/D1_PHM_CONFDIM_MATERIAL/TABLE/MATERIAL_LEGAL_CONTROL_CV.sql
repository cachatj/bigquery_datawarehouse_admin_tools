CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_LEGAL_CONTROL_CV`
(
  MATNR_MAEX STRING NOT NULL OPTIONS(description="Material Number; MAEX.MATNR where MAEX.MATNR = MATERIAL_CV.MATNR_MARA and MATERIAL_CV.CURR_VRSN_FLG = Y and MAEX.ALAND = US"),
  GEGRU_MAEX STRING NOT NULL OPTIONS(description="Legal Regulation for Legal Control ID"),
  BEZEI_T606R STRING OPTIONS(description="Legal Regulation Description Text; T606R.BEZEI where T606R.GEGRU = MAEX.GEGRU and T606R.SPRAS = E"),
  ALNUM_MAEX STRING NOT NULL OPTIONS(description="Export Control Class for Legal Control according to the Legal Regulation"),
  BEZEI_T606V STRING OPTIONS(description="Export Control Class Description Text; T606V.BEZEI where T606V.ALNUM = MAEX.ALNUM and T606V.GEGRU = MAEX.GEGRU and T606V.SPRAS = E"),
  EMBGR_MAEX STRING OPTIONS(description="Legal Control Grouping ID"),
  BEZEI_T606U STRING OPTIONS(description="Legal Control Grouping Description Text; T606U.BEZEI where T606U.EMBGR = MAEX.EMBGR and T606U.GEGRU = MAEX.GEGRU and T606U.SPRAS = E"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MATNR_MAEX, GEGRU_MAEX, ALNUM_MAEX;