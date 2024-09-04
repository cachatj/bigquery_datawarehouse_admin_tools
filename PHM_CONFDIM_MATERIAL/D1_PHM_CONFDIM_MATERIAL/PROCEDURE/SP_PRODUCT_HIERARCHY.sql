CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_PRODUCT_HIERARCHY()
BEGIN

/***********************************************************************************************************************
Author: Gopalakrishnan Thangavel
Creation Date: 05/12/2023
Data Sources: VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T179_CV,VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T179T_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_PRODUCT_HIERARCHY';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE T179T_TEMP
AS
SELECT
	T179.PRODH,
	CAST(T179.STUFE AS INT64) AS STUFE,
	T179T.VTEXT,
	v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T179_CV` T179
INNER JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T179T_CV` T179T ON T179T.PRODH = T179.PRODH
WHERE T179T.SPRAS = 'E'
AND T179T.D0_CURR_VRSN_FLG = 'Y'
AND T179.D0_CURR_VRSN_FLG = 'Y';

-- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE T179T_TEMP_INS
AS
SELECT
	PRODH,
	STUFE,
	VTEXT,
	TMP.ROW_ADD_STP,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID
FROM T179T_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PRODUCT_HIERARCHY_CV` TGT
ON TMP.PRODH = TGT.PRODH_T179
WHERE TGT.PRODH_T179 IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.PRODUCT_HIERARCHY_CV` TGT
SET STUFE_T179 = TMP.STUFE,
VTEXT_T179T = TMP.VTEXT,
TGT.ROW_UPDATE_STP = v_start_stp,
TGT.ROW_UPDATE_USER_ID = v_stored_proc_name
FROM T179T_TEMP TMP
WHERE TMP.PRODH = TGT.PRODH_T179;

-- Step 3: Insert records into the target

INSERT INTO D1_PHM_CONFDIM_MATERIAL.PRODUCT_HIERARCHY_CV
SELECT
	PRODH,
	STUFE,
	VTEXT,
	ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM T179T_TEMP_INS;

END;