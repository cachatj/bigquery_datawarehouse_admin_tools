CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICING_INDICATOR()
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICING_INDICATOR';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP
AS
SELECT
 DISTINCT(YYRETAIL)
,CASE WHEN YYRETAIL= '01' THEN 'RETAIL NO'
      WHEN YYRETAIL= '02' THEN 'PROMO NO'
	  WHEN YYRETAIL= '03' THEN 'RETAIL YES'
	  WHEN YYRETAIL= '04' THEN 'PROMO YES'
      ELSE 'NA' END AS RETAIL_PRICE_IND_DESC_TXT
FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__KNVV_CV` KNVV
WHERE D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP_INS
AS
SELECT
YYRETAIL
,TMP.RETAIL_PRICE_IND_DESC_TXT
FROM KNVV_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.RETAIL_PRICING_INDICATOR_CV` TGT
ON TMP.YYRETAIL = TGT.YYRETAIL_KNVV
WHERE TGT.YYRETAIL_KNVV IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.RETAIL_PRICING_INDICATOR_CV` TGT
SET
TGT.RETAIL_PRICE_IND_DESC_TXT = TMP.RETAIL_PRICE_IND_DESC_TXT
,ROW_UPDATE_STP=v_start_stp
FROM KNVV_TEMP TMP
WHERE TMP.YYRETAIL = TGT.YYRETAIL_KNVV;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.RETAIL_PRICING_INDICATOR_CV
SELECT
YYRETAIL
,RETAIL_PRICE_IND_DESC_TXT
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM KNVV_TEMP_INS;

/*Stored Proc execution logging for success or failure */

INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    'D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICING_INDICATOR',
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
	( SELECT 'D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICING_INDICATOR', current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;