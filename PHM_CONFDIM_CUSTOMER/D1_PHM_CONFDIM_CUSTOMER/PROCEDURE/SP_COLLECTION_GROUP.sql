CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_COLLECTION_GROUP()
BEGIN

/***********************************************************************************************************************
Author: Akshobhya S Achar
Creation Date: 05/09/2023
Data Sources: VI0_PHM_ORP_PE1_PH1_NP.UDM_COLL_GRP_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_COLLECTION_GROUP';

/* Select the latest record from the table */

CREATE OR REPLACE TEMPORARY TABLE UDM_COLL_GRP_TEMP
AS
SELECT
UDM_COLL_GRP.COLL_GROUP
,UDM_COLL_GRP.COLL_STRATEGY
,'' AS COLL_GROUP_TEXT

FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__UDM_COLL_GRP_CV` UDM_COLL_GRP
LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_ORP_PE1_PH1__UDM_COLL_GRP_CV` UDM_COLL_GRPT
ON UDM_COLL_GRPT.COLL_GROUP = UDM_COLL_GRP.COLL_GROUP
WHERE  UDM_COLL_GRP.D0_CURR_VRSN_FLG = 'Y';

/*Perform an UPSERT on the target table

Step 1: Identify records to be inserted */

CREATE OR REPLACE TEMPORARY TABLE UDM_COLL_GRP_TEMP_INS
AS
SELECT
TMP.COLL_GROUP
,TMP.COLL_STRATEGY
,'' AS COLL_GROUP_TEXT
FROM UDM_COLL_GRP_TEMP AS TMP
LEFT JOIN
`PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV`  AS TGT
ON TMP.COLL_GROUP = TGT.COLL_GROUP_UDM_COLL_GRP
WHERE TGT.COLL_GROUP_UDM_COLL_GRP IS NULL;

/*Step 2: Update records where the source and target match */

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV` TGT
SET
COLL_STRATEGY_UDM_COLL_GRP=  TMP.COLL_STRATEGY
,COLL_GROUP_TEXT_UDM_COLL_GRPT= TMP.COLL_GROUP_TEXT
,ROW_UPDATE_STP=v_start_stp
FROM UDM_COLL_GRP_TEMP TMP
WHERE TMP.COLL_GROUP = TGT.COLL_GROUP_UDM_COLL_GRP;

/*Step 3: Insert records into the target */

INSERT INTO D1_PHM_CONFDIM_CUSTOMER.COLLECTION_GROUP_CV
SELECT
COLL_GROUP
,COLL_STRATEGY
,COLL_GROUP_TEXT
,v_start_stp AS ROW_ADD_STP
,v_stored_proc_name AS ROW_ADD_USER_ID
,v_start_stp AS ROW_UPDATE_STP
,v_stored_proc_name AS ROW_UPDATE_USER_ID
FROM UDM_COLL_GRP_TEMP_INS;

END;