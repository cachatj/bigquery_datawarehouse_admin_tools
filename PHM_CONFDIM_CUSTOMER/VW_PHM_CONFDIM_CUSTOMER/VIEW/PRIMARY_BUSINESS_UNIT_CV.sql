CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.PRIMARY_BUSINESS_UNIT_CV`(
  CUSTOMER_PRIM_BU_ID OPTIONS(description="Customer Primary BU ID"),
  CUSTOMER_PRIM_BU_DESC_TXT OPTIONS(description="Customer Primary BU Description Text"),
  SALE_ORG_ID OPTIONS(description="Sales Organization ID"),
  DIST_CHNL_ID OPTIONS(description="Distribution Channel ID"),
  DIV_ID OPTIONS(description="Division ID"),
  PASS_THR_BU_CDE OPTIONS(description="Pass Through BU Code"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYPRIME_YTSD_PRIMARY_BU     AS     CUSTOMER_PRIM_BU_ID
,DESCR_YTSD_PRIMARY_BU     AS     CUSTOMER_PRIM_BU_DESC_TXT
,VKORG_YTSD_PRIMARY_BU     AS     SALE_ORG_ID
,VTWEG_YTSD_PRIMARY_BU     AS     DIST_CHNL_ID
,SPART_YTSD_PRIMARY_BU     AS     DIV_ID
,PASSTHRU_BU_YTSD_PRIMARY_BU     AS     PASS_THR_BU_CDE
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BUSINESS_UNIT_CV`;