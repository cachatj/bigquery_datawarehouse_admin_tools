CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.ITEM_PARTIAL_DELIVERY_CV`(
  ITEM_PRTL_DELIVERIES_ALW_ID OPTIONS(description="Item Partial Deliveries Allowed ID"),
  ITEM_PRTL_DELIVERIES_ALW_DESC_TXT OPTIONS(description="Item Partial Deliveries Allowed Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 KZTLF_KNVV     AS     ITEM_PRTL_DELIVERIES_ALW_ID
,ITEM_PRTL_DELIVERIES_ALW_DESC_TXT     AS     ITEM_PRTL_DELIVERIES_ALW_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.ITEM_PARTIAL_DELIVERY_CV`;