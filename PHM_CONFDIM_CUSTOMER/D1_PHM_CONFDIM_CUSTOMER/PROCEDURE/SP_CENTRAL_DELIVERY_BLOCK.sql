CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_CENTRAL_DELIVERY_BLOCK()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI0_PHM_ORP_PE1_PH1_NP.TVLS_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_CENTRAL_DELIVERY_BLOCK';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE TVLS_TEMP
AS
SELECT
TVLS.LIFSP
,TVLST.VTEXT
,TVLS.SPELF
,TVLS.SPEKO
,TVLS.SPEWA
,TVLS.SPEFT
,TVLS.SPEBE
,TVLS.SPEAU
,TVLS.SPEDR
,TVLS.SPEVI
,CAST(TVLS.MBDIF AS INT64) AS MBDIF
FROM  `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__TVLS_CV` TVLS
LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__TVLST_CV` TVLST

ON TVLS.LIFSP = TVLST.LIFSP
WHERE TVLS.D0_CURR_VRSN_FLG = 'Y'
AND TVLST.SPRAS = 'E';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE TVLS_TEMP_INS
AS
SELECT
LIFSP
,VTEXT
,SPELF
,SPEKO
,SPEWA
,SPEFT
,SPEBE
,SPEAU
,SPEDR
,SPEVI
,MBDIF
FROM TVLS_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK_CV` TGT
ON TMP.LIFSP = TGT.LIFSP_TVLS
WHERE TGT.LIFSP_TVLS IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK_CV` TGT
SET
VTEXT_TVLST= TMP.VTEXT
,SPELF_TVLS = TMP.SPELF
,SPEKO_TVLS = TMP.SPEKO
,SPEWA_TVLS = TMP.SPEWA
,SPEFT_TVLS = TMP.SPEFT
,SPEBE_TVLS = TMP.SPEBE
,SPEAU_TVLS = TMP.SPEAU
,SPEDR_TVLS = TMP.SPEDR
,SPEVI_TVLS = TMP.SPEVI
,MBDIF_TVLS = TMP.MBDIF
,ROW_UPDATE_STP=v_start_stp
FROM TVLS_TEMP TMP
WHERE TMP.LIFSP= TGT.LIFSP_TVLS;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.CENTRAL_DELIVERY_BLOCK_CV
SELECT
 LIFSP
,VTEXT
,SPELF
,SPEKO
,SPEWA
,SPEFT
,SPEBE
,SPEAU
,SPEDR
,SPEVI
,MBDIF
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM TVLS_TEMP_INS;

/*Stored Proc execution logging for success or failure */

INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    'D1_PHM_CONFDIM_SP.SP_CENTRAL_DELIVERY_BLOCK',
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
	( SELECT 'D1_PHM_CONFDIM_SP.SP_CENTRAL_DELIVERY_BLOCK', current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;