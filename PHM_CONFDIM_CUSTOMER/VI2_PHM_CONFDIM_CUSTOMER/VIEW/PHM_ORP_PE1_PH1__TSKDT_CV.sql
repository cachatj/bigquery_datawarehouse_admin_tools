CREATE VIEW `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__TSKDT_CV`
OPTIONS(
  friendly_name="PHM_ORP_PE1_PH1__TSKDT_CV"
)
AS SELECT MANDT, SPRAS, TATYP, TAXKD, VTEXT, ZSLTSTMP, D0_START_STP, D0_END_STP, D0_ADD_STP, D0_ADD_USER_ID, D0_UPDATE_STP, D0_UPDATE_USER_ID, D0_ACTION_ID, D0_SOURCE_ID, D0_CURR_VRSN_FLG, D0_SEQ_NUM FROM VI0_PHM_ORP_PE1_PH1_NP.TSKDT_CV;