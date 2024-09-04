CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV()
BEGIN
/*******************************************************************************************************************************************

Name: Shaik, Shareef
Date: Dec 23rd, 2023
Project: GPSC Iteration 2
Description: KOTG753 from D0 to D1;
Marker :
--------------------------------------------------------------------------------------------------
********************************************************************************************************************************************************/
DECLARE v_start_time datetime;
DECLARE v_start_stp timestamp;
DECLARE v_end_stp timestamp;
DECLARE v_stored_proc_name string;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV_TEMP
AS
SELECT v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID,
KOTG753.YYVKORG AS YYVKORG_KOTG753,
TV.VTEXT AS YYVTEXT_TVKOT,
KOTG753.YYVTWEG AS YYVTWEG_KOTG753,
WT.VTEXT AS YYVTEXT_TVTWT,
KOTG753.SPART AS SPART_KOTG753,
SP.VTEXT AS VTEXT_TSPAT
,IRM_CLNUM AS IRM_CLNUM_KOTG753
,DATBI AS DATBI_KOTG753
,DATAB AS DATAB_KOTG753
 FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__KOTG753_CV` KOTG753
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVKOT_CV` TV
 ON KOTG753.YYVKORG=TV.VKORG AND TV.SPRAS='E' AND KOTG753.D0_CURR_VRSN_FLG='Y'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVTWT_CV` WT
 ON KOTG753.YYVTWEG = WT.VTWEG AND WT.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TSPAT_CV` SP
 ON KOTG753.SPART = SP.SPART AND SP.SPRAS='E';

 -- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV_TEMP_INS
AS
SELECT
	TMP.YYVKORG_KOTG753
	,TMP.YYVTEXT_TVKOT
	,TMP.YYVTWEG_KOTG753
	,TMP.YYVTEXT_TVTWT
	,TMP.SPART_KOTG753
	,TMP.VTEXT_TSPAT
	,TMP.IRM_CLNUM_KOTG753
	,'Y' AS CURR_VRSN_FLG
	,TMP.DATBI_KOTG753
	,TMP.DATAB_KOTG753
	,TMP.ROW_ADD_STP ,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID
FROM CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL` KOTG753
ON TMP.YYVKORG_KOTG753 = KOTG753.YYVKORG_KOTG753 AND TMP.YYVTWEG_KOTG753 = KOTG753.YYVTWEG_KOTG753 AND TMP.SPART_KOTG753 = KOTG753.SPART_KOTG753 AND TMP.IRM_CLNUM_KOTG753 = KOTG753.IRM_CLNUM_KOTG753 AND TMP.DATBI_KOTG753 = KOTG753.DATBI_KOTG753 AND TMP.DATAB_KOTG753 = KOTG753.DATAB_KOTG753
WHERE
KOTG753.YYVKORG_KOTG753 IS NULL AND KOTG753.YYVTWEG_KOTG753  IS NULL AND KOTG753.SPART_KOTG753  IS NULL AND KOTG753.IRM_CLNUM_KOTG753  IS NULL ;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL` KOTG753
SET

	KOTG753.YYVKORG_KOTG753=TMP.YYVKORG_KOTG753
	,KOTG753.YYVTEXT_TVKOT=TMP.YYVTEXT_TVKOT
	,KOTG753.YYVTWEG_KOTG753=TMP.YYVTWEG_KOTG753
	,KOTG753.YYVTEXT_TVTWT=TMP.YYVTEXT_TVTWT
	,KOTG753.SPART_KOTG753=TMP.SPART_KOTG753
	,KOTG753.VTEXT_TSPAT=TMP.VTEXT_TSPAT
	,KOTG753.IRM_CLNUM_KOTG753=TMP.IRM_CLNUM_KOTG753
	,KOTG753.DATBI_KOTG753=TMP.DATBI_KOTG753
	,KOTG753.DATAB_KOTG753=TMP.DATAB_KOTG753
	,KOTG753.ROW_UPDATE_USER_ID= v_stored_proc_name
FROM CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV_TEMP TMP
WHERE TMP.YYVKORG_KOTG753 = KOTG753.YYVKORG_KOTG753 AND TMP.YYVTWEG_KOTG753 = KOTG753.YYVTWEG_KOTG753 AND TMP.SPART_KOTG753 = KOTG753.SPART_KOTG753 AND TMP.IRM_CLNUM_KOTG753 = KOTG753.IRM_CLNUM_KOTG753
AND TMP.DATBI_KOTG753 = KOTG753.DATBI_KOTG753 AND TMP.DATAB_KOTG753 = KOTG753.DATAB_KOTG753 ;

-- Step 3: Insert records into the target

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LOCAL_INCL_EXCL_CUSTLIST_MTRL`
SELECT
	YYVKORG_KOTG753
	,YYVTEXT_TVKOT
	,YYVTWEG_KOTG753
	,YYVTEXT_TVTWT
	,SPART_KOTG753
	,VTEXT_TSPAT
	,IRM_CLNUM_KOTG753
	,DATAB_KOTG753
	,DATBI_KOTG753
	,CURR_VRSN_FLG
	,ROW_ADD_STP
	,ROW_ADD_USER_ID
	,ROW_UPDATE_STP
	,ROW_UPDATE_USER_ID
FROM CUSTOMER_PHM_IRM_SNAPSHOT_KOTG753_CV_TEMP_INS;

END;