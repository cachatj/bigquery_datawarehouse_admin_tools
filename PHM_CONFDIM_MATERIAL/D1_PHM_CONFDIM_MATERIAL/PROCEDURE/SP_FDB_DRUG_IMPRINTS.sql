CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_IMPRINTS()
BEGIN

/***********************************************************************************************************************
Author: Gopalakrishnan Thangavel
Creation Date: 05/15/2023
Data Sources:

*************************************************************************
Change History		[DATE]			[CHANGED BY]	[JIRA#]		[CODE REVIEW BY]		[DESCRIPTION]
	<#>					<MM/DD/YYYY>	<Name>			<ID>

************************************************************************************************************************/

DECLARE v_start_time datetime;
DECLARE v_start_stp timestamp;
DECLARE v_end_stp timestamp;
DECLARE v_stored_proc_name string;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_IMPRINTS';

--flush the data and insert
TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_IMPRINTS_CV`;

CREATE OR REPLACE TEMPORARY TABLE FDB_MASTER_DRUG_IMPRINTS
AS
SELECT
  FDB_MASTER.NDC,
  A.IPTUNIQID,
  A.IPTDFID,
  A.IPTMFGID,
  A.IPTMFGNAME,
  A.IPTDFDESC,
  A.IPTPROPID,
  A.IPTLBLID,
  A.IPTSIDE1,
  A.IPTSIDE2,
  A.IPTTEXTID,
  A.IPTLINENO,
  A.IPTTEXT,
  A.CHG_FLAG,
  RANK() OVER(PARTITION BY FDB_MASTER.NDC ORDER BY FDB_MASTER.AUDIT.EVENTTIME DESC) AS RANK_RN
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_FDB__FDB_MASTER_CV`
LEFT JOIN UNNEST(FDB_MASTER.DRUG_IMPRINTS) AS A
;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_IMPRINTS_CV`
SELECT
  NDC,
  IPTUNIQID,
  IPTDFID,
  IPTMFGID,
  IPTMFGNAME,
  IPTDFDESC,
  IPTPROPID,
  IPTLBLID,
  IPTSIDE1,
  IPTSIDE2,
  IPTTEXTID,
  IPTLINENO,
  IPTTEXT,
  CHG_FLAG,
  v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_start_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID

FROM FDB_MASTER_DRUG_IMPRINTS
WHERE RANK_RN = 1
;

END;