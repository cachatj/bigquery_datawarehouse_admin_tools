CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.PACKAGING_INDICATOR_CV`(
  PACK_IND_ID OPTIONS(description="Packaging Indicator ID"),
  PACK_IND_DESC_TXT OPTIONS(description="Packaging Indicator Description Text"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 YYPCKG_YTMM_PACK_TEXT     AS     PACK_IND_ID
,YYDESC_YTMM_PACK_TEXT     AS     PACK_IND_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PACKAGING_INDICATOR_CV`;