CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_PRICING_VF`
(
  MATNR_A921 STRING NOT NULL OPTIONS(description="Material Number"),
  VRSN_START_DTE DATE NOT NULL OPTIONS(description="Version Start Date; Convert A921.DATAB to ISO date"),
  VRSN_END_DTE DATE OPTIONS(description="Version End Date; Convert A921.DATBI to ISO date"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag; Case when current_date between A921.DATAB and A921.DATBI then 'Y' else 'N' end"),
  ZBA0_KBETR_KONP NUMERIC OPTIONS(description="WAC Amount; Case when A921.KSCHL = 'ZBA0' then KONP.KBETR else 0 end"),
  ZBA2_KBETR_KONP NUMERIC OPTIONS(description="Discounted WAC Amount; Case when A921.KSCHL = 'ZBA2' then KONP.KBETR else 0 end"),
  ZTF0_KBETR_KONP NUMERIC OPTIONS(description="Excise Tax Amount; Case when A921.KSCHL = 'ZTF0' then KONP.KBETR else 0 end"),
  ZREF_KBETR_KONP NUMERIC OPTIONS(description="Reference Price (AWP) Amount; Case when A921.KSCHL = 'ZREF' then KONP.KBETR else 0 end"),
  ZMSR_KBETR_KONP NUMERIC OPTIONS(description="MSRP Amount; Case when A921.KSCHL = 'ZMSR' then KONP.KBETR else 0 end"),
  CALC_CORP_NIFO_AMT NUMERIC OPTIONS(description="Corporate NIFO Amount; IF MARA.YYPRIU= 'ZDIS' Then ZBA2+ZTFO Else ZBA0+ZTFO"),
  CALC_PREV_ZBA0_KBETR_KONP NUMERIC OPTIONS(description="Previous WAC Amount; LEAD(ZBA0_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZBA2_KBETR_KONP NUMERIC OPTIONS(description="Previous Discounted WAC Amount; LEAD(ZBA2_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZTF0_KBETR_KONP NUMERIC OPTIONS(description="Previous Excise Tax Amount; LEAD(ZTF0_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZREF_KBETR_KONP NUMERIC OPTIONS(description="Previous Reference Price (AWP) Amount; LEAD(ZREF_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_ZMSR_KBETR_KONP NUMERIC OPTIONS(description="Previous MSRP Amount; LEAD(ZMSR_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_CORP_NIFO_AMT NUMERIC OPTIONS(description="Previous Corporate NIFO Amount;IF MARA.YYPRIU= 'ZDIS' Then CALC_PREV_ZBA2_KBETR_KONP+CALC_PREV_ZTF0_KBETR_KONP Else CALC_PREV_ZBA0_KBETR_KONP+CALC_PREV_ZTF0_KBETR_KONP"),
  PD_ZBA7_KBETR_KONP NUMERIC OPTIONS(description="PD Chargeback Cost; KONP.KBETR where A910.KSCHL = ZBA7 and A910.SPART = 10 and A921.MATNR = A910.MATNR and A910.KNUMH = KONP.KNUMH"),
  KINRAY_ZBA7_KBETR_KONP NUMERIC OPTIONS(description="Kinray Chargeback Cost; KONP.KBETR where A910.KSCHL = ZBA7 and A910.SPART = 30 and A921.MATNR = A910.MATNR and A910.KNUMH = KONP.KNUMH"),
  CALC_PREV_PD_ZBA7_KBETR_KONP NUMERIC OPTIONS(description="PD Chargeback Cost; LEAD(PD_ZBA7_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_PREV_KINRAY_ZBA7_KBETR_KONP NUMERIC OPTIONS(description="Kinray Chargeback Cost; LEAD(KINRAY_ZBA7_KBETR_KONP, 1) OVER(PARTITION BY MATNR_A921 ORDER BY VRSN_START_DTE DESC)"),
  CALC_BASE_COST_AMT NUMERIC OPTIONS(description="Base Cost; IF MARA.YYPRIU= 'ZDIS' Then ZBA2 Else ZBA0"),
  CALC_PREV_BASE_COST_AMT NUMERIC OPTIONS(description="Previous Base Cost; IF MARA.YYPRIU= 'ZDIS' Then CALC_PREV_ZBA2_KBETR_KONP Else CALC_PREV_ZBA0_KBETR_KONP"),
  ZBA0_CRT_DTE DATE OPTIONS(description="WAC Create Date; Case when A921.KSCHL = 'ZBA0' then SAFE_CAST(KONH.ERDAT AS DATE FORMAT 'YYYYMMDD') else DATE '1900-01-01' end"),
  ZBA2_CRT_DTE DATE OPTIONS(description="Discounted WAC Create Date; Case when A921.KSCHL = 'ZBA2' then SAFE_CAST(KONH.ERDAT AS DATE FORMAT 'YYYYMMDD') else DATE '1900-01-01' end"),
  ZTF0_CRT_DTE DATE OPTIONS(description="Excise Tax Create Date; Case when A921.KSCHL = 'ZTF0' then SAFE_CAST(KONH.ERDAT AS DATE FORMAT 'YYYYMMDD') else DATE '1900-01-01' end"),
  ZREF_CRT_DTE DATE OPTIONS(description="Reference Price (AWP) Create Date; Case when A921.KSCHL = 'ZREF' then SAFE_CAST(KONH.ERDAT AS DATE FORMAT 'YYYYMMDD') else DATE '1900-01-01' end"),
  ZMSR_CRT_DTE DATE OPTIONS(description="MSRP Create Date; Case when A921.KSCHL = 'ZMSR' then SAFE_CAST(KONH.ERDAT AS DATE FORMAT 'YYYYMMDD') else DATE '1900-01-01' end"),
  CALC_CORP_NIFO_CRT_DTE DATE OPTIONS(description="Corporate NIFO Create Date; IF MARA.YYPRIU= 'ZDIS' Then ZBA2_CRT_DTE Else ZBA0_CRT_DTE"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
);