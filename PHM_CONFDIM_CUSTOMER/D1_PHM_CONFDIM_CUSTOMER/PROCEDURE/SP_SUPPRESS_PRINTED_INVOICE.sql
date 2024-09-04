CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_PRINTED_INVOICE()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI0_PHM_ORP_PE1_PH1_NP.KNVV_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_PRINTED_INVOICE';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP
AS
SELECT
 DISTINCT(YYSUPPRINV)
,CASE WHEN YYSUPPRINV= '01' THEN 'SUPPRESS ALL PRINTED INVOICES'
      WHEN YYSUPPRINV= '02' THEN 'SUPPRESS PRINTED INVOICES FOR NON-CONTROL ITEMS'
	  WHEN YYSUPPRINV= '03' THEN 'SUPPRESS ALL OUTPUT'
      ELSE 'NA' END AS PRINTED_INVOICE_SUPPRESSION_DESC_TXT
FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__KNVV_CV` KNVV
WHERE D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP_INS
AS
SELECT
YYSUPPRINV
,TMP.PRINTED_INVOICE_SUPPRESSION_DESC_TXT
FROM KNVV_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SUPPRESS_PRINTED_INVOICE_CV` TGT
ON TMP.YYSUPPRINV = TGT.YYSUPPRINV_KNVV
WHERE TGT.YYSUPPRINV_KNVV IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.SUPPRESS_PRINTED_INVOICE_CV` TGT
SET
PRINTED_INVOICE_SUPPRESSION_DESC_TXT = TMP.PRINTED_INVOICE_SUPPRESSION_DESC_TXT
,ROW_UPDATE_STP=v_start_stp
FROM KNVV_TEMP TMP
WHERE TMP.YYSUPPRINV= TGT.YYSUPPRINV_KNVV;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.SUPPRESS_PRINTED_INVOICE_CV
SELECT
YYSUPPRINV
,PRINTED_INVOICE_SUPPRESSION_DESC_TXT
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM KNVV_TEMP_INS;

/*Stored Proc execution logging for success or failure */

INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    'D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_PRINTED_INVOICE',
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
	( SELECT 'D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_PRINTED_INVOICE', current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;