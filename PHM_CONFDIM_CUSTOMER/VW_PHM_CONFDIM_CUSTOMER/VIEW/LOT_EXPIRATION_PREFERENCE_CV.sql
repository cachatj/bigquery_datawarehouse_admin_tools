CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.LOT_EXPIRATION_PREFERENCE_CV`(
  LOT_EXP_PREFER_ID OPTIONS(description="Lot Expiration Preference ID"),
  LOT_EXP_PREFER_DESC_TXT OPTIONS(description="Lot Expiration Preference Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYLOTEXP_YTSD_LOT_EXP_TXT     AS     LOT_EXP_PREFER_ID
,YYDES_YTSD_LOT_EXP_TXT     AS     LOT_EXP_PREFER_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.LOT_EXPIRATION_PREFERENCE_CV`;