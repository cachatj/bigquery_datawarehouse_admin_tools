CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__A714_CV`
CLONE `PROJECT_ID.D0_PHM_ORP_PE1_PH1_NP.A714_CV`
OPTIONS(
  labels=[("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T07:57:54.787Z";