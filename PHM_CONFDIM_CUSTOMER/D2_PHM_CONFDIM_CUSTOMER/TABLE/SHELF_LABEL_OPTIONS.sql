CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.SHELF_LABEL_OPTIONS`
(
  YYOESLOPT_KNVV STRING OPTIONS(description="Shelf Label Options ID;Distinct values of KNVV.YYOESLOPT"),
  SHELF_LABEL_OPTIONS_DESC_TXT STRING OPTIONS(description="Shelf Label Options Description Text;Case KNVV.YYOESLOPT when 09 then OE SHELF LABEL NDC/UPC BARCODE; when 10 then OE SHELF LABEL CIN BARCODE; else NA"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);