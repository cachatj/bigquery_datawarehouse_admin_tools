CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_DANGEROUS_GOOD_INDICATOR()
BEGIN

/***********************************************************************************************************************
Author: Gopalakrishnan Thangavel
Creation Date: 05/10/2023
Data Sources: VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TDG42_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_DANGEROUS_GOOD_INDICATOR';

-- Select the latest record from the table
CREATE OR REPLACE TEMPORARY TABLE TDG42_TEMP
AS
SELECT
	PROFL,
	PROFLD,
	v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TDG42_CV`
WHERE DGSPRAS = 'E'
AND D0_CURR_VRSN_FLG = 'Y';

-- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE TDG42_TEMP_INS
AS
SELECT
	PROFL,
	PROFLD,
	TMP.ROW_ADD_STP,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID
FROM TDG42_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.DANGEROUS_GOOD_INDICATOR_CV` TGT
ON TMP.PROFL = TGT.PROFL_TDG42
WHERE TGT.PROFL_TDG42 IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.DANGEROUS_GOOD_INDICATOR_CV` TGT
SET PROFLD_TDG42 = TMP.PROFLD,
	TGT.ROW_UPDATE_STP = v_start_stp,
	TGT.ROW_UPDATE_USER_ID = v_stored_proc_name
FROM TDG42_TEMP TMP
WHERE TMP.PROFL = TGT.PROFL_TDG42;

-- Step 3: Insert records into the target

INSERT INTO D1_PHM_CONFDIM_MATERIAL.DANGEROUS_GOOD_INDICATOR_CV
SELECT
	PROFL,
	PROFLD,
	ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM TDG42_TEMP_INS;

END;