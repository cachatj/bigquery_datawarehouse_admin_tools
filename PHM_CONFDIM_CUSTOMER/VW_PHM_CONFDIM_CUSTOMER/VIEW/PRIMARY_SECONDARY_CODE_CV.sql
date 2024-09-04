CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.PRIMARY_SECONDARY_CODE_CV`(
  PRIM_SCNDRY_ID OPTIONS(description="Primary Secondary ID"),
  PRIM_SCNDRY_DESC_TXT OPTIONS(description="Primary Secondary Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYPRISEC_YTSD_PRISEC_TEXT     AS     PRIM_SCNDRY_ID
,YYPRISEC_DES_YTSD_PRISEC_TEXT     AS     PRIM_SCNDRY_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_SECONDARY_CODE_CV`;