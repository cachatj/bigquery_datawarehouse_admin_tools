CREATE PROCEDURE `PROJECT_ID`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_DELTA_LOAD()
BEGIN
DECLARE
  last_load_timestamp timestamp;
SET
  last_load_timestamp = (
  SELECT
    IFNULL(MAX(creation_time),'2023-09-01 00:00:00')
  FROM
    `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_T);
INSERT INTO
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_JOBS_T
SELECT
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS snapshot_timestamp,
  *,
FROM
  `ednacomp-phm-np-cah.region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > last_load_timestamp;
END;