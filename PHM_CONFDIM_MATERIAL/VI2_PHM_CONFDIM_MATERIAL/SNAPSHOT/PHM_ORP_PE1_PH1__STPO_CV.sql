CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__STPO_CV`
CLONE `PROJECT_ID.D0_PHM_ORP_PE1_PH1_NP.STPO_CV`
OPTIONS(
  description="BOM item",
  labels=[("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T08:10:09.003Z";