CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_CUSTOMER.FUEL_SURCHARGE_CV`(
  FUEL_SURCHRG_ID OPTIONS(description="Fuel Surcharge ID"),
  FUEL_SURCHRG_DESC_TXT OPTIONS(description="Fuel Surcharge Description Text"),
  ROW_ADD_STP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID OPTIONS(description="Indicates by which job the row was updated")
)
AS SELECT
 YYFUELCHG_YTSD_FUELCHG_TXT     AS     FUEL_SURCHRG_ID
,YYFUELCHG_DES_YTSD_FUELCHG_TXT     AS     FUEL_SURCHRG_DESC_TXT
,ROW_ADD_STP     AS     ROW_ADD_STP
,ROW_ADD_USER_ID     AS     ROW_ADD_USER_ID
,ROW_UPDATE_STP     AS     ROW_UPDATE_STP
,ROW_UPDATE_USER_ID     AS     ROW_UPDATE_USER_ID
FROM `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.FUEL_SURCHARGE_CV`;