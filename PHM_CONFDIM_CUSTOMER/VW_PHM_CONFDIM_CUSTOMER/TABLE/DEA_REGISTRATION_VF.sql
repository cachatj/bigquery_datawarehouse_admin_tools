CREATE TABLE `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.DEA_REGISTRATION_VF`
(
  DEA_NUM STRING OPTIONS(description="DEA Registration Number"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag"),
  DEL_FLG STRING OPTIONS(description="Delete Flag"),
  VRSN_START_DTE DATE OPTIONS(description="Version Start Date"),
  VRSN_END_DTE DATE OPTIONS(description="Version End Date"),
  PYMT_IND_ID STRING OPTIONS(description="Payment Indicator ID"),
  PYMT_IND_DESC STRING OPTIONS(description="Payment Indicator Description"),
  BSNS_ACT_ID STRING OPTIONS(description="Business Activity Code"),
  BSNS_ACT_SUB_ID STRING OPTIONS(description="Business Activity Sub Code"),
  BSNS_ACT_DESC STRING OPTIONS(description="Business Activity Description"),
  DEA_RGSTRN_EXP_DTE DATE OPTIONS(description="DEA Registration Expiration Date"),
  CMPNY_NAM STRING OPTIONS(description="Company Name"),
  ADDL_CMPNY_INFO_TXT STRING OPTIONS(description="Additional Company Info"),
  ADDR_LINE_1_TXT STRING OPTIONS(description="Address1"),
  ADDR_LINE_2_TXT STRING OPTIONS(description="Address2"),
  CITY STRING OPTIONS(description="City"),
  STATE STRING OPTIONS(description="State"),
  DEA_SCHED_1_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_2_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_3_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_2N_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_3N_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_4_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_5_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  DEA_SCHED_L1_FLG STRING OPTIONS(description="Drug Schedule Flag"),
  PSTL_CDE STRING OPTIONS(description="Postal Code"),
  DEGREE STRING OPTIONS(description="Degree"),
  STATE_LICENSE_NUM STRING OPTIONS(description="State License Number"),
  STATE_CS_LICENSE_NUM STRING OPTIONS(description="State CS License Number"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
);