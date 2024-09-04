CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.RETAIL_PRICE_TIER_CV`(
  SALE_ORG_ID OPTIONS(description="Sales Organization ID"),
  DIST_CHNL_ID OPTIONS(description="Distribution Channel ID"),
  DIV_ID OPTIONS(description="Division ID"),
  RETAIL_PRICE_TIER_ID OPTIONS(description="Retail Price Tier ID"),
  RETAIL_PRICE_TIER_DESC_TXT OPTIONS(description="Retail Price Tier Description Text"),
  CREATE_BY_USER_ID OPTIONS(description="Created By User ID"),
  CREATE_ON_DTE OPTIONS(description="Created On Date"),
  CHG_BY_USER_ID OPTIONS(description="Changed By User ID"),
  CHG_ON_DTE OPTIONS(description="Changed On Date"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 VKORG_YTSD_HAMACHER_ZL     AS     SALE_ORG_ID
,VTWEG_YTSD_HAMACHER_ZL     AS     DIST_CHNL_ID
,SPART_YTSD_HAMACHER_ZL     AS     DIV_ID
,YYRPRPT_YTSD_HAMACHER_ZL     AS     RETAIL_PRICE_TIER_ID
,YYRPRPT_DESC_YTSD_HAMACHER_ZL     AS     RETAIL_PRICE_TIER_DESC_TXT
,ERNAM_YTSD_HAMACHER_ZL     AS     CREATE_BY_USER_ID
,ERDAT_YTSD_HAMACHER_ZL     AS     CREATE_ON_DTE
,AENAM_YTSD_HAMACHER_ZL     AS     CHG_BY_USER_ID
,AEDAT_YTSD_HAMACHER_ZL     AS     CHG_ON_DTE
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.RETAIL_PRICE_TIER_CV`;