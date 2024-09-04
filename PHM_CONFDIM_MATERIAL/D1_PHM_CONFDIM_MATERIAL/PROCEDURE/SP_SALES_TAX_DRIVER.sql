CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_SALES_TAX_DRIVER()
BEGIN

/***********************************************************************************************************************
Author: Gopalakrishnan Thangavel
Creation Date: 05/12/2023
Data Sources: VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__ZMT_PTS_TAX_DRVR_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_SALES_TAX_DRIVER';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE ZMT_PTS_TAX_DRVR_TEMP
AS
SELECT
	ZZMTAXD,
	ZZTAXGC,
	DESCRIPTION,
	v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__ZMT_PTS_TAX_DRVR_CV`
WHERE
D0_CURR_VRSN_FLG = 'Y';

-- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE ZMT_PTS_TAX_DRVR_TEMP_INS
AS
SELECT
	ZZMTAXD,
	ZZTAXGC,
	DESCRIPTION,
	TMP.ROW_ADD_STP,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID
FROM ZMT_PTS_TAX_DRVR_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SALES_TAX_DRIVER_CV` TGT
ON TMP.ZZMTAXD = TGT.ZZMTAXD_ZMT_PTS_TAX_DRVR
WHERE TGT.ZZMTAXD_ZMT_PTS_TAX_DRVR IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.SALES_TAX_DRIVER_CV` TGT
SET ZZTAXGC_ZMT_PTS_TAX_DRVR = TMP.ZZTAXGC,
DESCRIPTION_ZMT_PTS_TAX_DRVR = TMP.DESCRIPTION,
TGT.ROW_UPDATE_STP = v_start_stp,
TGT.ROW_UPDATE_USER_ID = v_stored_proc_name
FROM ZMT_PTS_TAX_DRVR_TEMP TMP
WHERE TMP.ZZMTAXD = TGT.ZZMTAXD_ZMT_PTS_TAX_DRVR;

-- Step 3: Insert records into the target

INSERT INTO D1_PHM_CONFDIM_MATERIAL.SALES_TAX_DRIVER_CV
SELECT
	ZZMTAXD,
	ZZTAXGC,
	DESCRIPTION,
	ROW_ADD_STP,
	ROW_ADD_USER_ID,
	ROW_UPDATE_STP,
	ROW_UPDATE_USER_ID
FROM ZMT_PTS_TAX_DRVR_TEMP_INS;

END;