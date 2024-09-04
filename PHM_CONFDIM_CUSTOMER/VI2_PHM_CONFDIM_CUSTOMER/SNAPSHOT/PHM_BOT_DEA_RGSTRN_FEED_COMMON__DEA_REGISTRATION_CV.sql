CREATE SNAPSHOT TABLE `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_BOT_DEA_RGSTRN_FEED_COMMON__DEA_REGISTRATION_CV`
CLONE `PROJECT_ID.D0_PHM_BOT_DEA_RGSTRN_FEED_COMMON.DEA_REGISTRATION_CV`
OPTIONS(
  friendly_name="DEA_REGISTRATION_CV",
  description="Contains data ingestion into DEA Registartion table",
  labels=[("costcenter", "1120000208"), ("refresh-frequency", "604800")]
)
FOR SYSTEM_TIME AS OF TIMESTAMP "2024-01-13T08:13:50.310Z";