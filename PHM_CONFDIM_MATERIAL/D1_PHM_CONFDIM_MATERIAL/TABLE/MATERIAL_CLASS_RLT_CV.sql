CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_CLASS_RLT_CV`
(
  MATNR_MARA STRING NOT NULL OPTIONS(description="Material Number"),
  CALC_CLASS_TYPE_ID STRING OPTIONS(description="Material Classification ID; Case AUSP_CV.ATINN when '0000000072' then 'DOCUMENTS_MISSING' when '0000000073' then 'DUPLICATE_MATERIAL' else 'NOT_AVAILABLE' end"),
  CALC_CLASS_CRCTRSTC_VALUE_ID STRING OPTIONS(description="Material Classification Characteristic Value; AUSP_CV.ATWRT"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
);