CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL`
(
  YYVKORG_KOTG753 STRING OPTIONS(description="Sales Organization"),
  YYVTEXT_TVKOT STRING OPTIONS(description="Sales Organization Description Text;VTEXT whereA715_CV.VKORG=PHM_ORP_PE1_PH1__TVKOT_CV.VKORG and PHM_ORP_PE1_PH1__TVKOT_CV.SPRAS=E"),
  YYVTWEG_KOTG753 STRING OPTIONS(description="Distribution Channel"),
  YYVTEXT_TVTWT STRING OPTIONS(description="Distribution Channel Description Text;VTEXT where A715.VTWEG = PHM_ORP_PE1_PH1__TVTWT_CV.VTWEG and PHM_ORP_PE1_PH1__TVTWT_CV.SPRAS=E"),
  SPART_KOTG753 STRING OPTIONS(description="Division"),
  VTEXT_TSPAT STRING OPTIONS(description="Division Description Text;VTEXT where A715.SPART = PHM_ORP_PE1_PH1__TSPAT_CV.SPART and PHM_ORP_PE1_PH1__TSPAT_CV.SPRAS=E"),
  IRM_CLNUM_KOTG753 STRING OPTIONS(description="Contract Number"),
  DATAB_KOTG753 DATE OPTIONS(description="Version Start Date"),
  DATBI_KOTG753 DATE OPTIONS(description="Version End Date"),
  CURR_VRSN_FLG STRING OPTIONS(description="Current Version Flag"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added By User"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Updated Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Updated By User"),
  MATNR_KOTG753 STRING OPTIONS(description="Material Number")
);