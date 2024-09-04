CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.C2_RESTRICTION_CV`(
  C2_RESTRICTION_ID OPTIONS(description="C2 Restriction ID"),
  C2_RESTRICTION_DESC_TXT OPTIONS(description="C2 Restriction Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KATR8_TVK8T     AS     C2_RESTRICTION_ID
,VTEXT_TVK8T     AS     C2_RESTRICTION_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.C2_RESTRICTION_CV`;