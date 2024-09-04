CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_MEDISPAN_AUDIT()
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_MEDISPAN_AUDIT';

--flush the data and insert
TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_AUDIT_CV`;

CREATE OR REPLACE TEMPORARY TABLE MEDISPAN_MASTER_AUDIT
AS
SELECT
	MEDISPAN_MASTER.NDC_UPC_HRI_ID,
	MEDISPAN_MASTER.AUDIT.EVENTID,
	MEDISPAN_MASTER.AUDIT.EVENTNAME,
	MEDISPAN_MASTER.AUDIT.SOURCESYSTEM,
	MEDISPAN_MASTER.AUDIT.EVENTTYPE,
	MEDISPAN_MASTER.AUDIT.EVENTTIME,
  RANK() OVER(PARTITION BY MEDISPAN_MASTER.NDC_UPC_HRI_ID ORDER BY MEDISPAN_MASTER.AUDIT.EVENTTIME DESC) AS RANK_RN
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_MDSP__MEDISPAN_MASTER_CV`
;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MEDISPAN_AUDIT_CV`
SELECT
  NDC_UPC_HRI_ID,
  EVENTID,
  EVENTNAME,
  SOURCESYSTEM,
  EVENTTYPE,
  EVENTTIME,
  v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_start_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM MEDISPAN_MASTER_AUDIT
WHERE RANK_RN = 1
;

END;