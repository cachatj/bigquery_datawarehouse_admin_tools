CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.CUSTOMER_PARTNER_FUNCTION_RLT`
(
  KUNNR_KNVP STRING OPTIONS(description="Customer Number"),
  VKORG_KNVP STRING OPTIONS(description="Sales Organization ID"),
  VTWEG_KNVP STRING OPTIONS(description="Distribution Channel ID"),
  SPART_KNVP STRING OPTIONS(description="Division ID"),
  PARVW_KNVP STRING OPTIONS(description="Partner Function ID"),
  VRSN_START_STP TIMESTAMP OPTIONS(description="Derived by data engineering"),
  VRSN_END_STP TIMESTAMP OPTIONS(description="Derived by data engineering"),
  CURR_VRSN_FLG STRING OPTIONS(description="Derived by data engineering"),
  VTEXT_TPART_PARTNER_FUNCTION_CODE STRING OPTIONS(description="Partner Function Description Text;PARTNER_FUNCTION_CODE.VTEXT_TPART where PARTNER_FUNCTION_CODE.PARVW_TPART = KNVP.PARVW"),
  KUNN2_KNVP STRING OPTIONS(description="Partner Function Customer Number"),
  PARZA_KNVP INT64 OPTIONS(description="Partner Counter Number"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);