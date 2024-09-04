CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.SHIPPING_CONDITIONS_CV`(
  SHIP_CNDTN_ID OPTIONS(description="Shipping Conditions ID"),
  SHIP_CNDTN_DESC_TXT OPTIONS(description="Shipping Conditions Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 VSBED_TVSBT     AS     SHIP_CNDTN_ID
,VTEXT_TVSBT     AS     SHIP_CNDTN_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SHIPPING_CONDITIONS_CV`;