CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__YTSD_CODENUMTXT_CV`
CLONE `PROJECT_ID.D0_PHM_ORP_PE1_PH1_NP.YTSD_CODENUMTXT_CV`
OPTIONS(
  labels=[("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T08:24:53.568Z";