CREATE TABLE `PROJECT_ID.D2_PHM_CONFDIM_CUSTOMER.RETAIL_PRICE_TIER`
(
  VKORG_YTSD_HAMACHER_ZL STRING OPTIONS(description="Sales Organization ID"),
  VTWEG_YTSD_HAMACHER_ZL STRING OPTIONS(description="Distribution Channel ID"),
  SPART_YTSD_HAMACHER_ZL STRING OPTIONS(description="Division ID"),
  YYRPRPT_YTSD_HAMACHER_ZL STRING OPTIONS(description="Retail Price Tier ID"),
  YYRPRPT_DESC_YTSD_HAMACHER_ZL STRING OPTIONS(description="Retail Price Tier Description Text"),
  ERNAM_YTSD_HAMACHER_ZL STRING OPTIONS(description="Created By User ID"),
  ERDAT_YTSD_HAMACHER_ZL DATE OPTIONS(description="Created On Date"),
  AENAM_YTSD_HAMACHER_ZL STRING OPTIONS(description="Changed By User ID"),
  AEDAT_YTSD_HAMACHER_ZL DATE OPTIONS(description="Changed On Date"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
);