CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BUSINESS_UNIT_CV`
(
  YYPRIME_YTSD_PRIMARY_BU STRING OPTIONS(description="Customer Primary BU ID"),
  DESCR_YTSD_PRIMARY_BU STRING OPTIONS(description="Customer Primary BU Description Text"),
  VKORG_YTSD_PRIMARY_BU STRING OPTIONS(description="Sales Organization ID"),
  VTWEG_YTSD_PRIMARY_BU STRING OPTIONS(description="Distribution Channel ID"),
  SPART_YTSD_PRIMARY_BU STRING OPTIONS(description="Division ID"),
  PASSTHRU_BU_YTSD_PRIMARY_BU STRING OPTIONS(description="Pass Through BU Code"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYPRIME_YTSD_PRIMARY_BU;