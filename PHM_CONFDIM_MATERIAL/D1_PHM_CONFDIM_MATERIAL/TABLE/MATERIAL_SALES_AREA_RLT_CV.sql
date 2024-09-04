CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_SALES_AREA_RLT_CV`
(
  MATNR_MVKE STRING NOT NULL OPTIONS(description="Material Number"),
  VKORG_MVKE STRING NOT NULL OPTIONS(description="Sales Organization"),
  VTWEG_MVKE STRING NOT NULL OPTIONS(description="Distribution Channel"),
  LVORM_MVKE STRING OPTIONS(description="Distribution Chain Deletion Indicator Flag; If MVKE.LVORM = X then Y else N"),
  VMSTA_MVKE STRING OPTIONS(description="Distribution Chain Specific Material Status ID"),
  VMSTB_TVMST STRING OPTIONS(description="Distribution Chain Specific Material Status Description Text; TVMST.VMSTB where TVMST.VMSTA = MVKE.VMSTA and TVMST.SPRAS = E"),
  VMSTD_MVKE DATE OPTIONS(description="Distribution Chain Specific Material Status Valid From Date; Convert to ISO format"),
  MTPOS_MVKE STRING OPTIONS(description="Item Category Group ID"),
  BEZEI_TPTMT STRING OPTIONS(description="Item Category Group Description Text; TPTMT.BEZEI where TPTMT.MTPOS = MVKE.MTPOS and TPTMT.SPRAS = E"),
  KONDM_MVKE STRING OPTIONS(description="Material Pricing Group ID"),
  KTGRM_MVKE STRING OPTIONS(description="Material Account Assignment Group (AAG) ID"),
  PRAT1_MVKE STRING OPTIONS(description="Web Enabled Flag; If MVKE.PRAT1 = X then Y else N"),
  PRAT3_MVKE STRING OPTIONS(description="SFDC Enabled Flag; If MVKE.PRAT3 = X then Y else N"),
  YYRMAT_MVKE STRING OPTIONS(description="Replacement Material Number"),
  YYCMAT_MVKE STRING OPTIONS(description="Closest Alternative Material Number"),
  YYFMAT_MVKE STRING OPTIONS(description="Formulary Substitute Material Number"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Added Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Added User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY MATNR_MVKE, VKORG_MVKE, VTWEG_MVKE, VMSTA_MVKE
OPTIONS(
  description=""
);