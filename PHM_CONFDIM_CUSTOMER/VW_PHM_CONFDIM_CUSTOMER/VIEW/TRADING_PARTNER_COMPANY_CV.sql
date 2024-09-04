CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV`(
  TRADE_PRTNR_CMPNY_ID OPTIONS(description="Trading Partner Company ID"),
  TRADE_PRTNR_CMPNY_NAM OPTIONS(description="Trading Partner Company Name"),
  CMPNY_ID_CNTRY_CDE OPTIONS(description="Company ID Country Code"),
  CMPNY_ID_NAM_2 OPTIONS(description="Company ID Name 2"),
  CMPNY_ID_STREET_ADDR OPTIONS(description="Company ID Street Address"),
  CMPNY_ID_PO_BOX OPTIONS(description="Company ID PO Box"),
  CMPNY_ID_ZIP OPTIONS(description="Company ID Zip"),
  CMPNY_ID_CITY OPTIONS(description="Company ID City"),
  CMPNY_ID_CRNCY OPTIONS(description="Company ID Currency"),
  CMPNY_ID_GROUP_CDE OPTIONS(description="Company ID Grouping Code"),
  WRT_LINE_ITEM OPTIONS(description="Write Line Items"),
  CMPNY_ID_LGL_STAT OPTIONS(description="Company ID Legal Status"),
  CMPNY_LGL_FORM OPTIONS(description="Company Legal Form"),
  INDUSTRIAL_SCTR OPTIONS(description="Industrial Sector"),
  MSTR_DATA_CMPNY_CDE OPTIONS(description="Master Data Company Code"),
  CNSLDT_CMPNY_IND OPTIONS(description="Consolidation Company Indicator"),
  CMPNY_2_STREET OPTIONS(description="Company 2 Street"),
  READ_PURCH_ORDER OPTIONS(description="Read Purchase Order"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 RCOMP_T880     AS     TRADE_PRTNR_CMPNY_ID
,NAME1_T880     AS     TRADE_PRTNR_CMPNY_NAM
,CNTRY_T880     AS     CMPNY_ID_CNTRY_CDE
,NAME2_T880     AS     CMPNY_ID_NAM_2
,STRET_T880     AS     CMPNY_ID_STREET_ADDR
,POBOX_T880     AS     CMPNY_ID_PO_BOX
,PSTLC_T880     AS     CMPNY_ID_ZIP
,CITY_T880     AS     CMPNY_ID_CITY
,CURR_T880     AS     CMPNY_ID_CRNCY
,MODCP_T880     AS     CMPNY_ID_GROUP_CDE
,GLSIP_T880     AS     WRT_LINE_ITEM
,RESTA_T880     AS     CMPNY_ID_LGL_STAT
,RFORM_T880     AS     CMPNY_LGL_FORM
,ZWEIG_T880     AS     INDUSTRIAL_SCTR
,MCOMP_T880     AS     MSTR_DATA_CMPNY_CDE
,LCCOMP_T880     AS     CNSLDT_CMPNY_IND
,STRT2_T880     AS     CMPNY_2_STREET
,INDPO_T880     AS     READ_PURCH_ORDER
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV`;