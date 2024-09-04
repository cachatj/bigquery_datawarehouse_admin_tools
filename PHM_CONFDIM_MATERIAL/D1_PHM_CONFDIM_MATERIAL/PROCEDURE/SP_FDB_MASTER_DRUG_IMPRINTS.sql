CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_MASTER_DRUG_IMPRINTS()
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_FDB_MASTER_DRUG_IMPRINTS';

--flush the data and insert
TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_IMPRINTS_CV`;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.FDB_DRUG_IMPRINTS_CV`
SELECT  DISTINCT
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
  v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_start_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID

FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_EDA_FDB__FDB_MASTER_CV`
CROSS JOIN UNNEST(FDB_MASTER.DRUG_IMPRINTS) AS A
;

/*Stored Proc execution logging for success or failure */
INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    v_stored_proc_name,
    v_start_time,
    current_datetime(),
    'SUCCESS',
    NULL,
    @@ROW_COUNT,
    NULL,
    NULL,
    @@script.bytes_processed/1024/1024,
    @@script.bytes_billed/1024/1024,
    @@script.job_id,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL); EXCEPTION
    WHEN ERROR THEN
	INSERT INTO `D1_PHM_DW_LOG.LOGTBL_DIMENSION`
	( module_name, start_date, end_date, status, err_msg, err_st_txt, err_st_trc )
	( SELECT v_stored_proc_name, current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;