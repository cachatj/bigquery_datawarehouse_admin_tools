CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.MATERIAL_PRICING_VF`(
  MTRL_NUM OPTIONS(description="Material Number"),
  VRSN_START_DTE OPTIONS(description="Version Start Date"),
  VRSN_END_DTE OPTIONS(description="Version End Date"),
  CURR_VRSN_FLG OPTIONS(description="Current Version Flag"),
  WAC_AMT OPTIONS(description="WAC Amount"),
  DISC_WAC_AMT OPTIONS(description="Discounted WAC Amount"),
  EXCISE_TAX_AMT OPTIONS(description="Excise Tax Amount"),
  RFRNC_PRICE_AMT OPTIONS(description="Reference Price (AWP) Amount"),
  MSRP_AMT OPTIONS(description="MSRP Amount"),
  CALC_CORP_NIFO_AMT OPTIONS(description="Corporate NIFO Amount"),
  CALC_PREV_WAC_AMT OPTIONS(description="Previous WAC Amount"),
  CALC_PREV_DISC_WAC_AMT OPTIONS(description="Previous Discounted WAC Amount"),
  CALC_PREV_EXCISE_TAX_AMT OPTIONS(description="Previous Excise Tax Amount"),
  CALC_PREV_RFRNC_PRICE_AMT OPTIONS(description="Previous Reference Price (AWP) Amount"),
  CALC_PREV_MSRP_AMT OPTIONS(description="Previous MSRP Amount"),
  CALC_PREV_CORP_NIFO_AMT OPTIONS(description="Previous Corporate NIFO Amount"),
  PD_CGBK_COST_AMT OPTIONS(description="PD Chargeback Cost"),
  KINRAY_CGBK_COST_AMT OPTIONS(description="KINRAY Chargeback Cost"),
  CALC_PREV_PD_CGBK_COST_AMT OPTIONS(description="Previous PD Chargeback Cost"),
  CALC_PREV_KINRAY_CGBK_COST_AMT OPTIONS(description="Previous KINRAY Chargeback Cost"),
  CALC_BASE_COST_AMT OPTIONS(description="Base Cost"),
  CALC_PREV_BASE_COST_AMT OPTIONS(description="Previous Base Cost"),
  WAC_CRT_DTE OPTIONS(description="WAC Amount Create Date"),
  DISC_WAC_CRT_DTE OPTIONS(description="Discounted WAC Amount Create Date"),
  EXCISE_TAX_CRT_DTE OPTIONS(description="Excise Tax Amount Create Date"),
  RFRNC_PRICE_CRT_DTE OPTIONS(description="Reference Price (AWP) Amount Create Date"),
  MSRP_CRT_DTE OPTIONS(description="MSRP Amount Create Date"),
  CALC_CORP_NIFO_CRT_DTE OPTIONS(description="Corporate NIFO Create Date"),
  ROW_ADD_STP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID OPTIONS(description="Row Update User ID")
)
AS SELECT
 MATNR_A921     AS     MTRL_NUM
,VRSN_START_DTE     AS     VRSN_START_DTE
,VRSN_END_DTE     AS     VRSN_END_DTE
,CURR_VRSN_FLG     AS     CURR_VRSN_FLG
,ZBA0_KBETR_KONP     AS     WAC_AMT
,ZBA2_KBETR_KONP     AS     DISC_WAC_AMT
,ZTF0_KBETR_KONP     AS     EXCISE_TAX_AMT
,ZREF_KBETR_KONP     AS     RFRNC_PRICE_AMT
,ZMSR_KBETR_KONP     AS     MSRP_AMT
,CALC_CORP_NIFO_AMT     AS     CALC_CORP_NIFO_AMT
,CALC_PREV_ZBA0_KBETR_KONP     AS     CALC_PREV_WAC_AMT
,CALC_PREV_ZBA2_KBETR_KONP     AS     CALC_PREV_DISC_WAC_AMT
,CALC_PREV_ZTF0_KBETR_KONP     AS     CALC_PREV_EXCISE_TAX_AMT
,CALC_PREV_ZREF_KBETR_KONP     AS     CALC_PREV_RFRNC_PRICE_AMT
,CALC_PREV_ZMSR_KBETR_KONP     AS     CALC_PREV_MSRP_AMT
,CALC_PREV_CORP_NIFO_AMT     AS     CALC_PREV_CORP_NIFO_AMT
,PD_ZBA7_KBETR_KONP          AS PD_CGBK_COST_AMT
,KINRAY_ZBA7_KBETR_KONP      AS KINRAY_CGBK_COST_AMT
,CALC_PREV_PD_ZBA7_KBETR_KONP AS CALC_PREV_PD_CGBK_COST_AMT
,CALC_PREV_KINRAY_ZBA7_KBETR_KONP AS CALC_PREV_KINRAY_CGBK_COST_AMT
,CALC_BASE_COST_AMT AS CALC_BASE_COST_AMT
,CALC_PREV_BASE_COST_AMT AS CALC_PREV_BASE_COST_AMT
,ZBA0_CRT_DTE AS WAC_CRT_DTE
,ZBA2_CRT_DTE AS DISC_WAC_CRT_DTE
,ZTF0_CRT_DTE AS EXCISE_TAX_CRT_DTE
,ZREF_CRT_DTE AS RFRNC_PRICE_CRT_DTE
,ZMSR_CRT_DTE AS MSRP_CRT_DTE
,CALC_CORP_NIFO_CRT_DTE AS CALC_CORP_NIFO_CRT_DTE
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_PRICING_VF`;