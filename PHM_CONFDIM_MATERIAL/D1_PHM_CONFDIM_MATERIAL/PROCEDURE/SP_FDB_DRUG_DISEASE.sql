CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_DISEASE()
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_DISEASE';

--flush the data and insert
TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_DISEASE_CV`;

CREATE OR REPLACE TEMPORARY TABLE FDB_MASTER_DRUG_DISEASE
AS
SELECT
  FDB_MASTER.NDC,
  A.DDXCN,
  A.DDXCN_SN,
  A.FDBDX,
  A.DDXCN_SL_DESC,
  A.DDXCN_REF,
  A.DXID,
  A.DXID_DESC56,
  A.DXID_DESC100,
  A.DXID_STATUS,
  A.DXID_DISEASE_DURATION_CD,
RANK() OVER(PARTITION BY FDB_MASTER.NDC ORDER BY FDB_MASTER.AUDIT.EVENTTIME DESC) AS RANK_RN
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_FDB__FDB_MASTER_CV`
LEFT JOIN UNNEST(FDB_MASTER.DRUG_DISEASE) AS A
;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_DISEASE_CV`
SELECT
  NDC,
  DDXCN,
  DDXCN_SN,
  FDBDX,
  DDXCN_SL_DESC,
  DDXCN_REF,
  DXID,
  DXID_DESC56,
  DXID_DESC100,
  DXID_STATUS,
  DXID_DISEASE_DURATION_CD,
  v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_start_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM FDB_MASTER_DRUG_DISEASE
WHERE RANK_RN = 1
;

END;