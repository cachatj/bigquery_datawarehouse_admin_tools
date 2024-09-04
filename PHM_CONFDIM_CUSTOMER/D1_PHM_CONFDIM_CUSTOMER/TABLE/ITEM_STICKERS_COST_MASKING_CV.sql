CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.ITEM_STICKERS_COST_MASKING_CV`
(
  YYCUSMASK_YTSD_CUSMASK_TXT STRING OPTIONS(description="Item Stickers Cost Masking ID;YTSD_CUSMASK_TXT.YYCUSMASK where YTSD_CUSMASK_TXT.SPRAS = E"),
  YYCUSMASK_DES_YTSD_CUSMASK_TXT STRING OPTIONS(description="Item Stickers Cost Masking Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YYCUSMASK_YTSD_CUSMASK_TXT;