CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CREDIT_CONTROL_AREA_CV`(
  CREDIT_CNTRL_AREA_ID OPTIONS(description="Credit Control Area ID"),
  CREDIT_CNTRL_AREA_DESC_TXT OPTIONS(description="Credit Control Area Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KKBER_T014T     AS     CREDIT_CNTRL_AREA_ID
,KKBTX_T014T     AS     CREDIT_CNTRL_AREA_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CREDIT_CONTROL_AREA_CV`;