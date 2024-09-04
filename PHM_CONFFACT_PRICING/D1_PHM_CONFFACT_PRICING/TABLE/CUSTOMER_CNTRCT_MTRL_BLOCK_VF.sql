CREATE TABLE `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_MTRL_BLOCK_VF`
(
  KNUMA_AG_A715 STRING OPTIONS(description="Agreement Number"),
  VKORG_A715 STRING OPTIONS(description="Sales Organization"),
  VTEXT_TVKOT STRING OPTIONS(description="Sales Organization Description Text;VTEXT whereA715_CV.VKORG=PHM_ORP_PE1_PH1__TVKOT_CV.VKORG and PHM_ORP_PE1_PH1__TVKOT_CV.SPRAS=E"),
  VTWEG_A715 STRING OPTIONS(description="Distribution Channel"),
  VTEXT_TVTWT STRING OPTIONS(description="Distribution Channel Description Text;VTEXT where A715.VTWEG = PHM_ORP_PE1_PH1__TVTWT_CV.VTWEG and PHM_ORP_PE1_PH1__TVTWT_CV.SPRAS=E"),
  SPART_A715 STRING OPTIONS(description="Division"),
  VTEXT_TSPAT STRING OPTIONS(description="Division Description Text;VTEXT where A715.SPART = PHM_ORP_PE1_PH1__TSPAT_CV.SPART and PHM_ORP_PE1_PH1__TSPAT_CV.SPRAS=E"),
  KUNAG_A715 STRING OPTIONS(description="Sold To Customer Number"),
  IRM_PCNUM_A715 STRING OPTIONS(description="Contract Number"),
  MATNR_A715 STRING OPTIONS(description="Material Number"),
  DATAB_A715 DATE OPTIONS(description="Version Start Date"),
  DATBI_A715 DATE OPTIONS(description="Version End Date"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added By User"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Updated By User"),
  LOEVM_KO_KONP STRING OPTIONS(description="Deletion Indicator Flag; If KONP.LOEVM_KO = X then Y else N where A717.KNUMH = KONP.KNUMH")
);