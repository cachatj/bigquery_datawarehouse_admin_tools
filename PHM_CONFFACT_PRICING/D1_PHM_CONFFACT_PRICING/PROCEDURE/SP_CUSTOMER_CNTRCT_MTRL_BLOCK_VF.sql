CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_MTRL_BLOCK_VF()
BEGIN
/*******************************************************************************************************************************************

Name: Shaik, Shareef
Date: Dec 23rd, 2023
Project: GPSC Iteration 2
Description: A715 from D0 to D1;
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
SET v_stored_proc_name = 'D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_MTRL_BLOCK_VF';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_CNTRCT_MTRL_BLOCK_VF_TEMP
AS
SELECT v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID,
  a.* FROM
(SELECT DISTINCT A715.KNUMA_AG AS KNUMA_AG_A715,
A715.VKORG AS VKORG_A715,
TV.VTEXT AS VTEXT_TVKOT,
A715.VTWEG AS VTWEG_A715,
WT.VTEXT AS VTEXT_TVTWT,
A715.SPART AS SPART_A715,
SP.VTEXT AS VTEXT_TSPAT,
A715.KUNAG AS KUNAG_A715
,A715.IRM_PCNUM AS IRM_PCNUM_A715
,A715.MATNR AS MATNR_A715
,PARSE_DATE('%Y%m%d', A715.DATBI ) AS DATBI_A715
,PARSE_DATE('%Y%m%d',A715.DATAB ) AS DATAB_A715
,CASE WHEN IFNULL(KONP.LOEVM_KO,'N') = 'X' THEN 'Y' ELSE 'N' END AS LOEVM_KO_KONP
--,KONP.LOEVM_KO
 FROM `PROJECT_ID.VI2_PHM_CONFFACT_PRICING.PHM_ORP_PE1_PH1__A715_CV` A715
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVKOT_CV` TV
 ON A715.VKORG=TV.VKORG AND TV.SPRAS='E' AND A715.D0_CURR_VRSN_FLG='Y'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVTWT_CV` WT
 ON A715.VTWEG = WT.VTWEG AND WT.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TSPAT_CV` SP
 ON A715.SPART = SP.SPART AND SP.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__KONP_CV` KONP
 ON A715.KNUMH=KONP.KNUMH)a ;

 -- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_CNTRCT_MTRL_BLOCK_VF_TEMP_INS
AS
SELECT
	TMP.KNUMA_AG_A715
	,TMP.VKORG_A715
	,TMP.VTEXT_TVKOT
	,TMP.VTWEG_A715
	,TMP.VTEXT_TVTWT
	,TMP.SPART_A715
	,TMP.VTEXT_TSPAT
	,TMP.KUNAG_A715
	,TMP.IRM_PCNUM_A715
	,'Y' AS CURR_VRSN_FLG
	,TMP.MATNR_A715
	,TMP.DATBI_A715
	,TMP.DATAB_A715
	,TMP.ROW_ADD_STP ,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID,
  TMP.LOEVM_KO_KONP
FROM CUSTOMER_CNTRCT_MTRL_BLOCK_VF_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_MTRL_BLOCK_VF` A715
ON TMP.KNUMA_AG_A715 = A715.KNUMA_AG_A715 AND TMP.SPART_A715 = A715.SPART_A715 AND TMP.KUNAG_A715 = A715.KUNAG_A715 AND TMP.IRM_PCNUM_A715 = A715.IRM_PCNUM_A715
AND TMP.MATNR_A715 = A715.MATNR_A715 AND TMP.DATBI_A715 = A715.DATBI_A715 AND TMP.DATAB_A715 = A715.DATAB_A715
WHERE
A715.KNUMA_AG_A715 IS NULL AND A715.SPART_A715  IS NULL AND A715.KUNAG_A715  IS NULL AND A715.IRM_PCNUM_A715  IS NULL AND A715.MATNR_A715  IS NULL AND A715.DATBI_A715  IS NULL AND
A715.DATAB_A715  IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_MTRL_BLOCK_VF` A715
SET
	A715.KNUMA_AG_A715=TMP.KNUMA_AG_A715
	,A715.VKORG_A715=TMP.VKORG_A715
	,A715.VTEXT_TVKOT=TMP.VTEXT_TVKOT
	,A715.VTWEG_A715=TMP.VTWEG_A715
	,A715.VTEXT_TVTWT=TMP.VTEXT_TVTWT
	,A715.SPART_A715=TMP.SPART_A715
	,A715.VTEXT_TSPAT=TMP.VTEXT_TSPAT
	,A715.KUNAG_A715=TMP.KUNAG_A715
	,A715.IRM_PCNUM_A715=TMP.IRM_PCNUM_A715
	,A715.MATNR_A715=TMP.MATNR_A715
	,A715.DATBI_A715=TMP.DATBI_A715
	,A715.DATAB_A715=TMP.DATAB_A715
	,A715.ROW_UPDATE_USER_ID= v_stored_proc_name
FROM CUSTOMER_CNTRCT_MTRL_BLOCK_VF_TEMP TMP
WHERE TMP.KNUMA_AG_A715 = A715.KNUMA_AG_A715 AND TMP.SPART_A715 = A715.SPART_A715 AND TMP.KUNAG_A715 = A715.KUNAG_A715 AND TMP.IRM_PCNUM_A715 = A715.IRM_PCNUM_A715
AND TMP.MATNR_A715 = A715.MATNR_A715 AND TMP.DATBI_A715 = A715.DATBI_A715 AND TMP.DATAB_A715 = A715.DATAB_A715 ;

-- Step 3: Insert records into the target

INSERT INTO `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_MTRL_BLOCK_VF`
SELECT
	KNUMA_AG_A715
	,VKORG_A715
  ,VTEXT_TVKOT
	,VTWEG_A715
  ,VTEXT_TVTWT
	,SPART_A715
  ,VTEXT_TSPAT
	,KUNAG_A715
	,IRM_PCNUM_A715
	,MATNR_A715
	,DATAB_A715
	,DATBI_A715
	,CURR_VRSN_FLG
	,ROW_ADD_STP
	,ROW_ADD_USER_ID
	,ROW_UPDATE_STP
	,ROW_UPDATE_USER_ID
  ,LOEVM_KO_KONP
FROM CUSTOMER_CNTRCT_MTRL_BLOCK_VF_TEMP_INS;

END;