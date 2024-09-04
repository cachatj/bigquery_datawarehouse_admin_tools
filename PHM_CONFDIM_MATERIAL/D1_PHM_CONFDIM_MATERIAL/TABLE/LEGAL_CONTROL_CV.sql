CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LEGAL_CONTROL_CV`
(
  GEGRU_T606V STRING OPTIONS(description="Legal Control ID; T606V.GEGRU where T606V.SPRAS = E"),
  ALNUM_T606V STRING OPTIONS(description="Export Control Class ID"),
  BEZEI_T606V STRING OPTIONS(description="Legal Control Export Control Class Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY GEGRU_T606V;