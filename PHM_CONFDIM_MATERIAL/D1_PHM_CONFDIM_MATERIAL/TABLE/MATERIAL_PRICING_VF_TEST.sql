CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_PRICING_VF_TEST`
(
  MATNR_A921 STRING NOT NULL OPTIONS(description="Material Number"),
  VRSN_START_DTE DATE NOT NULL OPTIONS(description="Version Start Date; Convert A921.DATAB to ISO date"),
  VRSN_END_DTE DATE OPTIONS(description="Version End Date; Convert A921.DATBI to ISO date"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag"),
  ZBA0_KBETR_KONP NUMERIC OPTIONS(description="WAC Amount"),
  ZBA2_KBETR_KONP NUMERIC OPTIONS(description="Discounted WAC Amount"),
  ZTF0_KBETR_KONP NUMERIC OPTIONS(description="Excise Tax Amount"),
  ZREF_KBETR_KONP NUMERIC OPTIONS(description="Reference Price (AWP) Amount"),
  ZMSR_KBETR_KONP NUMERIC OPTIONS(description="MSRP Amount"),
  CALC_CORP_NIFO_AMT NUMERIC OPTIONS(description="Corporate NIFO Amount; ZBA0 + ZTF0"),
  CALC_PREV_ZBA0_KBETR_KONP NUMERIC OPTIONS(description="Previous WAC Amount; LEAD(ZBA0_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZBA2_KBETR_KONP NUMERIC OPTIONS(description="Previous Discounted WAC Amount; LEAD(ZBA2_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZTF0_KBETR_KONP NUMERIC OPTIONS(description="Previous Excise Tax Amount; LEAD(ZTF0_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZREF_KBETR_KONP NUMERIC OPTIONS(description="Previous Reference Price (AWP) Amount; LEAD(ZREF_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZMSR_KBETR_KONP NUMERIC OPTIONS(description="Previous MSRP Amount; LEAD(ZMSR_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_CORP_NIFO_AMT NUMERIC OPTIONS(description="Previous Corporate NIFO Amount; Previous ZBA0 + Previous ZTF0"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MATNR_A921, VRSN_END_DTE, CURR_VRSN_FLG;