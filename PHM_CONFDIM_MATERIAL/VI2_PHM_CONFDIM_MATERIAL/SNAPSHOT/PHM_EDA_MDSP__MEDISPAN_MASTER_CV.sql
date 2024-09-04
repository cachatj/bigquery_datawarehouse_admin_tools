CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_MDSP__MEDISPAN_MASTER_CV`
CLONE `PROJECT_ID.D0_PHM_EDA_MDSP.MEDISPAN_MASTER_CV`
OPTIONS(
  friendly_name="MEDISPAN_MASTER_CV",
  labels=[("costcenter", "1120000208"), ("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-18T08:20:38.398Z";