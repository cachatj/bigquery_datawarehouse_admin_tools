CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.CUSTOMER_STOCK_NUMBER_CODE_CV`(
  CUSTOMER_STOCK_NUM_ID OPTIONS(description="Customer Stock Number ID"),
  CUSTOMER_STOCK_NUM_DESC_TXT OPTIONS(description="Customer Stock Number Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYCOMMODITY_KNVV     AS     CUSTOMER_STOCK_NUM_ID
,CUSTOMER_STOCK_NUM_DESC_TXT     AS     CUSTOMER_STOCK_NUM_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CUSTOMER_STOCK_NUMBER_CODE_CV`;