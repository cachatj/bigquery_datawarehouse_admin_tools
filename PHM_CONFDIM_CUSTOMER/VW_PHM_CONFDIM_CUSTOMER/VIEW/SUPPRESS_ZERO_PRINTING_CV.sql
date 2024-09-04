CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.SUPPRESS_ZERO_PRINTING_CV`(
  SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_ID OPTIONS(description="Suppress Printing All Zero Dollar Invoices ID"),
  SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_DESC_TXT OPTIONS(description="Suppress Printing All Zero Dollar Invoices Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYSUPZEROINV_KNVV     AS     SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_ID
,SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_DESC_TXT     AS     SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SUPPRESS_ZERO_PRINTING_CV`;