CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_RETAIL_TIER_PRICE_VF`
(
  MATNR_A714 STRING NOT NULL OPTIONS(description="Material Number"),
  VKORG_A714 STRING NOT NULL OPTIONS(description="Sales Organization ID"),
  VTWEG_A714 STRING NOT NULL OPTIONS(description="Distribution Channel ID"),
  SPART_A714 STRING NOT NULL OPTIONS(description="Division ID"),
  YYRPRPT_A714 STRING NOT NULL OPTIONS(description="Retail Price Tier ID"),
  YYRPRPT_DESC_YTSD_HAMACHER_ZL STRING OPTIONS(description="Retail Price Tier Description Text; YTSD_HAMACHER_ZL.YYRPRPT_DESC where YTSD_HAMACHER_ZL.YYRPRPT = A714.YYRPRPT"),
  DATAB_A714 DATE OPTIONS(description="Version Start Date; Convert A714.DATAB to ISO format"),
  DATBI_A714 DATE NOT NULL OPTIONS(description="Version End Date; Convert A714.DATBI to ISO format"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag; Y for the latest record, N for the rest"),
  KBETR_KONP NUMERIC OPTIONS(description="Retail Tier Price Amount; KONP.KBETR where KONP.KNUMH = A714.KNUMH"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
PARTITION BY DATAB_A714
CLUSTER BY MATNR_A714, VKORG_A714, VTWEG_A714, SPART_A714;