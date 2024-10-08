CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_BUY_GRP_MTRL_BLOCK_VF()
BEGIN
/*******************************************************************************************************************************************

Name: Shaik, Shareef
Date: Dec 23rd, 2023
Project: GPSC Iteration 2
Description: A716 from D0 to D1;
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
SET v_stored_proc_name = 'D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_BUY_GRP_SUPPLIER_BLOCK_VF';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_BUY_GRP_MTRL_BLOCK_VF_TEMP
AS
SELECT v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID,
a.* FROM (SELECT DISTINCT A716.KNUMA_AG AS KNUMA_AG_A716,
A716.VKORG AS VKORG_A716,
TV.VTEXT AS VTEXT_TVKOT,
A716.VTWEG AS VTWEG_A716,
WT.VTEXT AS VTEXT_TVTWT,
A716.SPART AS SPART_A716,
SP.VTEXT AS VTEXT_TSPAT,
A716.KUNAG AS KUNAG_A716
,A716.IRM_PCBGP_PRI AS IRM_PCBGP_PRI_A716
,A716.MATNR AS MATNR_A716
,PARSE_DATE('%Y%m%d', A716.DATBI ) AS DATBI_A716
,PARSE_DATE('%Y%m%d',A716.DATAB ) AS DATAB_A716
,CASE WHEN ifnull(KONP.LOEVM_KO,'N') = 'X' THEN 'Y' ELSE 'N' END AS LOEVM_KO_KONP
 FROM `PROJECT_ID.VI2_PHM_CONFFACT_PRICING.PHM_ORP_PE1_PH1__A716_CV` A716
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVKOT_CV` TV
 ON A716.VKORG=TV.VKORG AND TV.SPRAS='E' AND A716.D0_CURR_VRSN_FLG='Y'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVTWT_CV` WT
 ON A716.VTWEG = WT.VTWEG AND WT.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TSPAT_CV` SP
 ON A716.SPART = SP.SPART AND SP.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__KONP_CV` KONP
 ON A716.KNUMH=KONP.KNUMH)a ;

 -- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_BUY_GRP_MTRL_BLOCK_VF_TEMP_INS
AS
SELECT
	TMP.KNUMA_AG_A716
	,TMP.VKORG_A716
	,TMP.VTEXT_TVKOT
	,TMP.VTWEG_A716
	,TMP.VTEXT_TVTWT
	,TMP.SPART_A716
	,TMP.VTEXT_TSPAT
	,TMP.KUNAG_A716
	,TMP.IRM_PCBGP_PRI_A716
	,'Y' AS CURR_VRSN_FLG
	,TMP.MATNR_A716
	,TMP.DATBI_A716
	,TMP.DATAB_A716
	,TMP.ROW_ADD_STP ,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID,
  TMP.LOEVM_KO_KONP
FROM CUSTOMER_BUY_GRP_MTRL_BLOCK_VF_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_BUY_GRP_MTRL_BLOCK_VF` A716
ON TMP.KNUMA_AG_A716 = A716.KNUMA_AG_A716 AND TMP.SPART_A716 = A716.SPART_A716 AND TMP.KUNAG_A716 = A716.KUNAG_A716 AND TMP.IRM_PCBGP_PRI_A716 = A716.IRM_PCBGP_PRI_A716
AND TMP.MATNR_A716 = A716.MATNR_A716 AND TMP.DATBI_A716 = A716.DATBI_A716 AND TMP.DATAB_A716 = A716.DATAB_A716
WHERE
A716.KNUMA_AG_A716 IS NULL AND A716.SPART_A716  IS NULL AND A716.KUNAG_A716  IS NULL AND A716.IRM_PCBGP_PRI_A716  IS NULL AND A716.MATNR_A716  IS NULL AND A716.DATBI_A716  IS NULL AND
A716.DATAB_A716  IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_BUY_GRP_MTRL_BLOCK_VF` A716
SET
	A716.KNUMA_AG_A716=TMP.KNUMA_AG_A716
	,A716.VKORG_A716=TMP.VKORG_A716
	,A716.VTEXT_TVKOT=TMP.VTEXT_TVKOT
	,A716.VTWEG_A716=TMP.VTWEG_A716
	,A716.VTEXT_TVTWT=TMP.VTEXT_TVTWT
	,A716.SPART_A716=TMP.SPART_A716
	,A716.VTEXT_TSPAT=TMP.VTEXT_TSPAT
	,A716.KUNAG_A716=TMP.KUNAG_A716
	,A716.IRM_PCBGP_PRI_A716=TMP.IRM_PCBGP_PRI_A716
	,A716.MATNR_A716=TMP.MATNR_A716
	,A716.DATBI_A716=TMP.DATBI_A716
	,A716.DATAB_A716=TMP.DATAB_A716
	,A716.ROW_UPDATE_USER_ID= v_stored_proc_name
FROM CUSTOMER_BUY_GRP_MTRL_BLOCK_VF_TEMP TMP
WHERE TMP.KNUMA_AG_A716 = A716.KNUMA_AG_A716 AND TMP.SPART_A716 = A716.SPART_A716 AND TMP.KUNAG_A716 = A716.KUNAG_A716 AND TMP.IRM_PCBGP_PRI_A716 = A716.IRM_PCBGP_PRI_A716
AND TMP.MATNR_A716 = A716.MATNR_A716 AND TMP.DATBI_A716 = A716.DATBI_A716 AND TMP.DATAB_A716 = A716.DATAB_A716 ;

-- Step 3: Insert records into the target

INSERT INTO `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_BUY_GRP_MTRL_BLOCK_VF`
SELECT
	KNUMA_AG_A716
	,VKORG_A716
  ,VTEXT_TVKOT
	,VTWEG_A716
  ,VTEXT_TVTWT
	,SPART_A716
  ,VTEXT_TSPAT
	,KUNAG_A716
	,IRM_PCBGP_PRI_A716
	,MATNR_A716
	,DATAB_A716
	,DATBI_A716
	,CURR_VRSN_FLG
	,ROW_ADD_STP
	,ROW_ADD_USER_ID
	,ROW_UPDATE_STP
	,ROW_UPDATE_USER_ID
  ,LOEVM_KO_KONP
FROM CUSTOMER_BUY_GRP_MTRL_BLOCK_VF_TEMP_INS;

END;