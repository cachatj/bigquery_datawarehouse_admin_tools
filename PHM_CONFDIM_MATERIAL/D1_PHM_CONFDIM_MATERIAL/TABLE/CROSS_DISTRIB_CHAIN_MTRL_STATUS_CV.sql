CREATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.CROSS_DISTRIB_CHAIN_MTRL_STATUS_CV`
(
  VMSTA_TVMST STRING OPTIONS(description="Cross Distribution Chain Material Status ID; TVMST.VMSTA where TVMST.SPRAS = E"),
  VMSTB_TVMST STRING OPTIONS(description="Cross Distribution Chain Material Status Description Text"),
  ROW_ADD_STP TIMESTAMP OPTIONS(description="Row Add Timestamp"),
  ROW_ADD_USER_ID STRING OPTIONS(description="Row Add User ID"),
  ROW_UPDATE_STP TIMESTAMP OPTIONS(description="Row Update Timestamp"),
  ROW_UPDATE_USER_ID STRING OPTIONS(description="Row Update User ID")
)
CLUSTER BY VMSTA_TVMST;