CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MARA_CV`
CLONE `PROJECT_ID.D0_PHM_ORP_PE1_PH1_NP.MARA_CV`
OPTIONS(
  description="Product Master Genral data",
  labels=[("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T08:05:02.860Z";