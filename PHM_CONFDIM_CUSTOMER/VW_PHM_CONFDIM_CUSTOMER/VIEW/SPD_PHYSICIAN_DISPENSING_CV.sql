CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.SPD_PHYSICIAN_DISPENSING_CV`(
  SPD_PHYSCN_DSPNS_ID OPTIONS(description="SPD Physician Dispensing ID"),
  SPD_PHYSCN_DSPNS_DESC_TXT OPTIONS(description="SPD Physician Dispensing Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYPHYDISP_YTSD_PHYDIS_TEXT     AS     SPD_PHYSCN_DSPNS_ID
,YYPHYDISP_DES_YTSD_PHYDIS_TEXT     AS     SPD_PHYSCN_DSPNS_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SPD_PHYSICIAN_DISPENSING_CV`;