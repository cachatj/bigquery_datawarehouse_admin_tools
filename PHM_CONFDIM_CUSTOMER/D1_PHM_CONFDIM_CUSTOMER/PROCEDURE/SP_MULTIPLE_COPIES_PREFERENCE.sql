CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_MULTIPLE_COPIES_PREFERENCE()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__KNVV_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_MULTIPLE_COPIES_PREFERENCE';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP
AS
SELECT DISTINCT
SAFE_CAST (YYMULTCOP AS INT64) AS YYMULTCOP
,CASE WHEN YYMULTCOP= '1' THEN 'ONE COPY'
      WHEN YYMULTCOP= '2' THEN 'TWO COPIES'
      ELSE 'NA' END AS MULTI_COPY_CUSTOMER_PREFER_DESC_TXT
FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__KNVV_CV` KNVV
WHERE D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE KNVV_TEMP_INS
AS
SELECT
YYMULTCOP
,TMP.MULTI_COPY_CUSTOMER_PREFER_DESC_TXT
FROM KNVV_TEMP TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.MULTIPLE_COPIES_PREFERENCE_CV` TGT
ON TMP.YYMULTCOP = TGT.YYMULTCOP_KNVV
WHERE TGT.YYMULTCOP_KNVV IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.MULTIPLE_COPIES_PREFERENCE_CV` TGT
SET
TGT.MULTI_COPY_CUSTOMER_PREFER_DESC_TXT = TMP.MULTI_COPY_CUSTOMER_PREFER_DESC_TXT
FROM KNVV_TEMP TMP
WHERE TMP.YYMULTCOP = TGT.YYMULTCOP_KNVV;

/*Step 3: Insert records into the target */

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.MULTIPLE_COPIES_PREFERENCE_CV`
SELECT
YYMULTCOP
,MULTI_COPY_CUSTOMER_PREFER_DESC_TXT
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM KNVV_TEMP_INS;

END;