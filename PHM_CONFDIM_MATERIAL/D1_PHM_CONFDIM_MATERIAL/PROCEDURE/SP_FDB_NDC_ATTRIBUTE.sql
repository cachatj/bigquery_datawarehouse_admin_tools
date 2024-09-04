CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_NDC_ATTRIBUTE()
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_FDB_NDC_ATTRIBUTE';

--flush the data and insert
TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_NDC_ATTRIBUTE_CV`;

CREATE OR REPLACE TEMPORARY TABLE FDB_MASTER_NDC_ATTRIBUTE
AS
SELECT
  FDB_MASTER.NDC,
  A.NDC_ATTRIBUTE_TYPE_CD,
  A.NDC_ATTRIBUTE_TYPE_DSC,
  A.NDC_ATTRIBUTE_SN,
  A.NDC_ATTRIBUTE_VALUE,
  A.NDC_ATTRIBUTE_VALUE_DESC,
  RANK() OVER(PARTITION BY FDB_MASTER.NDC ORDER BY FDB_MASTER.AUDIT.EVENTTIME DESC) AS RANK_RN
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_FDB__FDB_MASTER_CV`
LEFT JOIN UNNEST(FDB_MASTER.NDC_ATTRIBUTE) AS A
;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_NDC_ATTRIBUTE_CV`
SELECT
  NDC,
  NDC_ATTRIBUTE_TYPE_CD,
  NDC_ATTRIBUTE_TYPE_DSC,
  NDC_ATTRIBUTE_SN,
  NDC_ATTRIBUTE_VALUE,
  NDC_ATTRIBUTE_VALUE_DESC,
  v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_start_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM FDB_MASTER_NDC_ATTRIBUTE
WHERE RANK_RN = 1
;

END;