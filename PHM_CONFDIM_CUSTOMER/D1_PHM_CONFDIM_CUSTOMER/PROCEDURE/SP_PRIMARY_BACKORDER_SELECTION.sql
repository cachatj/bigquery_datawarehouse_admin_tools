CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI2_PHM_ORP_PE1_PH1_NP.YTSD_BOPRE1_TEXT_CV
*************************************************************************
Change History		[DATE]			[CHANGED BY]	[JIRA#]		[CODE REVIEW BY]		[DESCRIPTION]
	<#>					<MM/DD/YYYY>	<Name>			<ID>

************************************************************************************************************************/

DECLARE v_start_time DATETIME;
DECLARE v_start_stp TIMESTAMP;
DECLARE v_end_stp TIMESTAMP;
DECLARE v_end_scd_stp TIMESTAMP;
DECLARE v_stored_proc_name STRING;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE YTSD_BOPRE1_TEXT_TEMP
AS
SELECT
DISTINCT
YYBOPRE1
,YYBOPRE1_DES
FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__YTSD_BOPRE1_TEXT_CV` YTSD_BOPRE1_TEXT
WHERE YTSD_BOPRE1_TEXT.SPRAS = 'E'
AND D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE YTSD_BOPRE1_TEXT_TEMP_INS
AS
SELECT
 YYBOPRE1
,YYBOPRE1_DES

FROM YTSD_BOPRE1_TEXT_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV` TGT
ON TMP.YYBOPRE1 = TGT.YYBOPRE1_YTSD_BOPRE1_TEXT
WHERE TGT.YYBOPRE1_YTSD_BOPRE1_TEXT IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV` TGT
SET
YYBOPRE1_DES_YTSD_BOPRE1_TEXT = TMP.YYBOPRE1_DES,
ROW_UPDATE_STP=v_start_stp
FROM YTSD_BOPRE1_TEXT_TEMP TMP
WHERE TMP.YYBOPRE1 = TGT.YYBOPRE1_YTSD_BOPRE1_TEXT;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.PRIMARY_BACKORDER_SELECTION_CV
SELECT
 YYBOPRE1
,YYBOPRE1_DES
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM YTSD_BOPRE1_TEXT_TEMP_INS;

/*Stored Proc execution logging for success or failure */

INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    'D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION',
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
	( SELECT 'D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION', current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;