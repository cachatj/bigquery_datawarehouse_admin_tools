CREATE VIEW `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_SAPERP__CHPDPE_CD_PARAM_CV`
OPTIONS(
  friendly_name="PHM_ORP_SAPERP__CHPDPE_CD_PARAM_CV"
)
AS SELECT MANDT, KUNNR, VKORG, VTWEG, SPART, TRADING_PTR, ACTIVE, NHIN_FLG, AWP_FLG, INV_FLG, SUP_340B_FLG, PRIORITY, INCODE, SINCODE, SI_TWCODE, SI_NOOFDAY, SI_NOOFMTH, INV_DAYS, CAT_TYPE1, SCOPT1, PROFILE_ID1, DEF_EXEC1, CAT_TYPE2, SCOPT2, PROFILE_ID2, DEF_EXEC2, ERDAT, ERNAM, ERZET, AEDAT, AENAM, AEZET, CAT_ACC_TYPE, D0_START_STP, D0_END_STP, D0_ADD_STP, D0_ADD_USER_ID, D0_UPDATE_STP, D0_UPDATE_USER_ID, D0_ACTION_ID, D0_SOURCE_ID, D0_CURR_VRSN_FLG, D0_SEQ_NUM FROM VI0_PHM_ORP_SAPERP_NP.CHPDPE_CD_PARAM_CV;