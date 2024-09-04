CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SUPPRESS_ZERO_PRINTING_CV`
(
  YYSUPZEROINV_KNVV STRING OPTIONS(description="Suppress Printing All Zero Dollar Invoices ID;Distinct values of KNVV.YYSUPZEROINV"),
  SPRS_PRINTING_ALL_ZERO_DLR_INVOICE_DESC_TXT STRING OPTIONS(description="Suppress Printing All Zero Dollar Invoices Description Text;Case KNVV.YYSUPZEROINV when 01 then SUPPRESS PRINT ONLY; when 02 then SUPPRESS ALL DELIVERY METHODS; when 03 then SUPPRESS ALL DELIVERY METHODS EXCEPT EDI; else NA"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYSUPZEROINV_KNVV;