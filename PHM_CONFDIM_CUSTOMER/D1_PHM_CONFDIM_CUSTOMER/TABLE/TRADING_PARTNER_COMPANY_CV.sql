CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV`
(
  RCOMP_T880 STRING OPTIONS(description="Trading Partner Company ID;T880.RCOMP where T880.LANGU = E"),
  NAME1_T880 STRING OPTIONS(description="Trading Partner Company Name"),
  CNTRY_T880 STRING OPTIONS(description="Company ID Country Code"),
  NAME2_T880 STRING OPTIONS(description="Company ID Name 2"),
  STRET_T880 STRING OPTIONS(description="Company ID Street Address"),
  POBOX_T880 STRING OPTIONS(description="Company ID PO Box"),
  PSTLC_T880 STRING OPTIONS(description="Company ID Zip"),
  CITY_T880 STRING OPTIONS(description="Company ID City"),
  CURR_T880 STRING OPTIONS(description="Company ID Currency"),
  MODCP_T880 STRING OPTIONS(description="Company ID Grouping Code"),
  GLSIP_T880 STRING OPTIONS(description="Write Line Items"),
  RESTA_T880 STRING OPTIONS(description="Company ID Legal Status"),
  RFORM_T880 STRING OPTIONS(description="Company Legal Form"),
  ZWEIG_T880 STRING OPTIONS(description="Industrial Sector"),
  MCOMP_T880 STRING OPTIONS(description="Master Data Company Code"),
  LCCOMP_T880 STRING OPTIONS(description="Consolidation Company Indicator"),
  STRT2_T880 STRING OPTIONS(description="Company 2 Street"),
  INDPO_T880 STRING OPTIONS(description="Read Purchase Order"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY RCOMP_T880;