CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__TVSBT_CV`
CLONE `PROJECT_ID.D0_PHM_ORP_PE1_PH1_NP.TVSBT_CV`
OPTIONS(
  labels=[("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T08:23:43.250Z";