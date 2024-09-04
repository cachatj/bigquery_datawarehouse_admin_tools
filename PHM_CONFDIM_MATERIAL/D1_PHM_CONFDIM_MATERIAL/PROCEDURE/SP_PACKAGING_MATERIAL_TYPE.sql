CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_PACKAGING_MATERIAL_TYPE()
BEGIN

/***********************************************************************************************************************
Author: Gopalakrishnan Thangavel
Creation Date: 05/10/2023
Data Sources: VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TVTYT_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_PACKAGING_MATERIAL_TYPE';
-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE TVTYT_TEMP
AS
SELECT
	TRATY,
	VTEXT,
	v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TVTYT_CV`
WHERE SPRAS = 'E'
AND D0_CURR_VRSN_FLG = 'Y';

-- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE TVTYT_TEMP_INS
AS
SELECT
	TRATY,
	VTEXT,
	TMP.ROW_ADD_STP,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID
FROM TVTYT_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV` TGT
ON TMP.TRATY = TGT.TRATY_TVTYT
WHERE TGT.TRATY_TVTYT IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV` TGT
SET VTEXT_TVTYT = TMP.VTEXT,
TGT.ROW_UPDATE_STP = v_start_stp,
TGT.ROW_UPDATE_USER_ID = v_stored_proc_name
FROM TVTYT_TEMP TMP
WHERE TMP.TRATY = TGT.TRATY_TVTYT;

-- Step 3: Insert records into the target

INSERT INTO D1_PHM_CONFDIM_MATERIAL.PACKAGING_MATERIAL_TYPE_CV
SELECT
	TRATY,
	VTEXT,
	ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM TVTYT_TEMP_INS;

END;