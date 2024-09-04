CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK_CV`(
  CENT_DLVRY_BLCK_ID OPTIONS(description="Central Delivery Block ID,"),
  CENT_DLVRY_BLCK_DESC_TXT OPTIONS(description="Central Delivery Block Description Text"),
  DLVRY_BLCK_ID OPTIONS(description="Delivery Block ID,"),
  PICK_BLCK_ID OPTIONS(description="Picking Block ID,"),
  GI_BLCK_ID OPTIONS(description="Goods Issue Block ID,"),
  BLCK_PRDCTN_ID OPTIONS(description="Block Production ID,"),
  CONFIRM_BLCK_ID OPTIONS(description="Confirmation Block ID,"),
  SALE_ORDER_BLCK_ID OPTIONS(description="Sales Order Block ID,"),
  PRINTING_BLCK_ID OPTIONS(description="Printing Block ID,"),
  DLVRY_DUE_LIST_BLCK_ID OPTIONS(description="Delivery Due List Block ID,"),
  MTRL_PRVSN_DEFERMENT_PERIOD_DAY_QTY OPTIONS(description="Material Provision Deferment Period Day Quantity"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 LIFSP_TVLS     AS     CENT_DLVRY_BLCK_ID
,VTEXT_TVLST     AS     CENT_DLVRY_BLCK_DESC_TXT
,SPELF_TVLS     AS     DLVRY_BLCK_ID
,SPEKO_TVLS     AS     PICK_BLCK_ID
,SPEWA_TVLS     AS     GI_BLCK_ID
,SPEFT_TVLS     AS     BLCK_PRDCTN_ID
,SPEBE_TVLS     AS     CONFIRM_BLCK_ID
,SPEAU_TVLS     AS     SALE_ORDER_BLCK_ID
,SPEDR_TVLS     AS     PRINTING_BLCK_ID
,SPEVI_TVLS     AS     DLVRY_DUE_LIST_BLCK_ID
,MBDIF_TVLS     AS     MTRL_PRVSN_DEFERMENT_PERIOD_DAY_QTY
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK_CV`;