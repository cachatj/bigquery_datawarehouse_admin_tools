CREATE VIEW `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__EMBK_CV`
OPTIONS(
  friendly_name="PHM_ORP_PE1_PH1__EMBK_CV"
)
AS SELECT MANDT, GENNR, INDEI, GEGRU, GEART, EXGEN, GENST, ANFDA, ANFUZ, ABGDA, ABGUZ, EIGDA, EIGUZ, VELDA, VELUZ, REODA, REOUZ, GST_0, GST_A, GST_B, GST_C, GST_D, GST_Z, GANDA, GENDA, GMAXW, WAERS, GMAXQ, MEINS, ERNAM, ERDAT, ERZET, GENLA, EXART, GAUFW, GLIEW, BUKRS, ZTERM, GMAXA, GAUFA, GPRZW, GAUFQ, GLIEQ, VBELN, VBDFG, VBDNR, BEHO2, BEHOE1, BEHOE2, ZSLTSTMP, D0_START_STP, D0_END_STP, D0_ADD_STP, D0_ADD_USER_ID, D0_UPDATE_STP, D0_UPDATE_USER_ID, D0_ACTION_ID, D0_SOURCE_ID, D0_CURR_VRSN_FLG, D0_SEQ_NUM FROM VI0_PHM_ORP_PE1_PH1_NP.EMBK_CV;