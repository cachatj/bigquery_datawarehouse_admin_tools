CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SUPPRESS_PRINTED_INVOICE`
(
  YYSUPPRINV_KNVV STRING OPTIONS(description="Printed Invoice Suppression ID;Distinct values of KNVV.YYSUPPRINV"),
  PRINTED_INVOICE_SUPPRESSION_DESC_TXT STRING OPTIONS(description="Printed Invoice Suppression Description Text;Case KNVV.YYSUPPRINV when 01 then SUPPRESS ALL PRINTED INVOICES; when 02 then SUPPRESS PRINTED INVOICES FOR NON-CONTROL ITEMS; when 03 then SUPPRESS ALL OUTPUT; else NA"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);