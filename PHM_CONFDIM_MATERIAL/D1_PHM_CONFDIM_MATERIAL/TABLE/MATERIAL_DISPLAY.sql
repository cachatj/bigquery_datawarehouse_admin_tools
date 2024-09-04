CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_DISPLAY`
(
  MATNR_MAST STRING NOT NULL OPTIONS(description="Display Material Number"),
  MAKTX_MAKT STRING OPTIONS(description="Display Material Description Text;  MAKT.MAKTX where MAKT.MATNR = MAST.MATNR and MAKT.SPRAS = E"),
  IDNRK_STPO STRING NOT NULL OPTIONS(description="Component Material Number"),
  MAKTX_MAKT_COMP STRING OPTIONS(description="Component Material Description Text; MAKT.MAKTX where MAKT.MATNR = STPO.IDNRK and MAKT.SPRAS = E"),
  MENGE_STPO NUMERIC OPTIONS(description="Component Material Quantity"),
  MEINS_STPO STRING OPTIONS(description="Component Unit Of Measure ID"),
  MSEHL_T006A STRING OPTIONS(description="Component Unit Of Measure Description Text; T006A.MSEHL where STPO .MEINS= T006A.MSEHI and T006A.SPRAS = E"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Updated User ID")
)
CLUSTER BY MATNR_MAST, IDNRK_STPO
OPTIONS(
  description=""
);