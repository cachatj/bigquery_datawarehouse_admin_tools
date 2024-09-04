CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_TRADING_PARTNER_COMPANY()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI0_PHM_ORP_PE1_PH1_NP.T880_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_TRADING_PARTNER_COMPANY';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE T880_TEMP
AS
SELECT
 RCOMP
,NAME1
,CNTRY
,NAME2
,STRET
,POBOX
,PSTLC
,CITY
,CURR
,MODCP
,GLSIP
,RESTA
,RFORM
,ZWEIG
,MCOMP
,LCCOMP
,STRT2
,INDPO
FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__T880_CV` T880
WHERE T880.LANGU = 'E'
AND D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE T880_TEMP_INS
AS
SELECT
 RCOMP
,NAME1
,CNTRY
,NAME2
,STRET
,POBOX
,PSTLC
,CITY
,CURR
,MODCP
,GLSIP
,RESTA
,RFORM
,ZWEIG
,MCOMP
,LCCOMP
,STRT2
,INDPO
FROM T880_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV` TGT
ON TMP.RCOMP = TGT.RCOMP_T880
WHERE TGT.RCOMP_T880 IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV` TGT
SET
 RCOMP_T880   = TMP.RCOMP
,NAME1_T880   = TMP.NAME1
,CNTRY_T880   = TMP.CNTRY
,NAME2_T880   = TMP.NAME2
,STRET_T880   = TMP.STRET
,POBOX_T880   = TMP.POBOX
,PSTLC_T880   = TMP.PSTLC
,CITY_T880    = TMP.CITY
,CURR_T880    = TMP.CURR
,MODCP_T880   = TMP.MODCP
,GLSIP_T880   = TMP.GLSIP
,RESTA_T880   = TMP.RESTA
,RFORM_T880   = TMP.RFORM
,ZWEIG_T880   = TMP.ZWEIG
,MCOMP_T880   = TMP.MCOMP
,LCCOMP_T880  = TMP.LCCOMP
,STRT2_T880   = TMP.STRT2
,INDPO_T880   = TMP.INDPO
,ROW_UPDATE_STP=v_start_stp
FROM T880_TEMP TMP
WHERE TMP.RCOMP = TGT.RCOMP_T880;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.TRADING_PARTNER_COMPANY_CV
SELECT
 RCOMP
,NAME1
,CNTRY
,NAME2
,STRET
,POBOX
,PSTLC
,CITY
,CURR
,MODCP
,GLSIP
,RESTA
,RFORM
,ZWEIG
,MCOMP
,LCCOMP
,STRT2
,INDPO
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM T880_TEMP_INS;

/*Stored Proc execution logging for success or failure */

INSERT INTO
  `PROJECT_ID.D1_PHM_DW_LOG.LOGTBL_DIMENSION` (
  SELECT
    'D1_PHM_CONFDIM_CUSTOMER.SP_TRADING_PARTNER_COMPANY',
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
	( SELECT 'D1_PHM_CONFDIM_CUSTOMER.SP_TRADING_PARTNER_COMPANY', current_datetime(), current_datetime(),
	'FAILED', @@error.message, @@error.statement_text, @@error.formatted_stack_trace );
END;