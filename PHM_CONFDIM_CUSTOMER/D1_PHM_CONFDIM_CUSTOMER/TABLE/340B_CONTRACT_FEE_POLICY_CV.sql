CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.340B_CONTRACT_FEE_POLICY_CV`
(
  YY340BCONFEE_YTSD_340BCONFTXT STRING OPTIONS(description="340B Contract Fee Policy ID, YTSD_340BCONFTXT.YY340BCONFEE where YTSD_340BCONFTXT.SPRAS = E"),
  YY340BCONFEE_DES_YTSD_340BCONFTXT STRING OPTIONS(description="Contract Fee Policy 340B Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Indicates date the row was inserted"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Indicates by which job the row was inserted"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Indicates date the row was updated"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Indicates by which job the row was updated")
)
CLUSTER BY YY340BCONFEE_YTSD_340BCONFTXT;