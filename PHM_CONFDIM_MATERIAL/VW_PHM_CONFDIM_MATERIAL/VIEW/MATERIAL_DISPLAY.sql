CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_DISPLAY`(
  DISP_MTRL_NUM OPTIONS(description="Display Material Number"),
  DISP_MTRL_DESC_TXT OPTIONS(description="Display Material Description Text"),
  COMP_MTRL_NUM OPTIONS(description="Component Material Number"),
  COMP_MTRL_DESC_TXT OPTIONS(description="Component Material Description Text"),
  COMP_MTRL_QTY OPTIONS(description="Component Material Quantity"),
  COMP_UOM_ID OPTIONS(description="Component Unit Of Measure ID"),
  COMP_UOM_DESC_TXT OPTIONS(description="Component Unit Of Measure Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Updated User ID")
)
OPTIONS(
  description=""
)
AS SELECT
   MATNR_MAST         AS MTRL_NUM,
   MAKTX_MAKT         AS MTRL_DESC_TXT,
   IDNRK_STPO         AS COMP_MTRL_NUM,
   MAKTX_MAKT_COMP    AS COMP_MTRL_DESC_TXT,
   MENGE_STPO         AS COMP_MTRL_QTY,
   MEINS_STPO         AS COMP_UOM_ID,
   MSEHL_T006A        AS COMP_UOM_DESC_TXT,
   ROW_ADD_STP        AS ROW_ADD_STP,
   ROW_ADD_USER_ID    AS ROW_ADD_USER_ID,
   ROW_UPDATE_STP     AS ROW_UPDATE_STP,
   ROW_UPDATE_USER_ID AS ROW_UPDATE_USER_ID
FROM
   PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_DISPLAY;