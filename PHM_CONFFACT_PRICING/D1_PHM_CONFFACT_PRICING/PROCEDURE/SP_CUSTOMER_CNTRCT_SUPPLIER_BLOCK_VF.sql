CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF()
BEGIN
/*******************************************************************************************************************************************

Name: Shaik, Shareef
Date: Dec 23rd, 2023
Project: GPSC Iteration 2
Description: A719 from D0 to D1;
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
SET v_stored_proc_name = 'D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF';

-- Select the latest record from the table

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_TEMP
AS
SELECT v_start_stp AS ROW_ADD_STP,
	v_stored_proc_name AS ROW_ADD_USER_ID,
	v_end_stp AS ROW_UPDATE_STP,
	v_stored_proc_name AS ROW_UPDATE_USER_ID,
	a.* FROM (SELECT DISTINCT
A719.KNUMA_AG AS KNUMA_AG_A719,
A719.VKORG AS VKORG_A719,
TV.VTEXT AS VTEXT_TVKOT,
A719.VTWEG AS VTWEG_A719,
WT.VTEXT AS VTEXT_TVTWT,
A719.SPART AS SPART_A719,
SP.VTEXT AS VTEXT_TSPAT,
A719.KUNAG AS KUNAG_A719
,A719.IRM_PCNUM AS IRM_PCNUM_A719
,A719.LIFNR AS LIFNR_A719
,PARSE_DATE('%Y%m%d', A719.DATBI ) AS DATBI_A719
,PARSE_DATE('%Y%m%d',A719.DATAB ) AS DATAB_A719
,CASE WHEN IFNULL(KONP.LOEVM_KO,'N') = 'X' THEN 'Y' ELSE 'N' END AS LOEVM_KO_KONP
 FROM `PROJECT_ID.VI2_PHM_CONFFACT_PRICING.PHM_ORP_PE1_PH1__A719_CV` A719
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVKOT_CV` TV
 ON A719.VKORG=TV.VKORG AND TV.SPRAS='E' AND A719.D0_CURR_VRSN_FLG='Y'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TVTWT_CV` WT
 ON A719.VTWEG = WT.VTWEG AND WT.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFFACT_BILLING.PHM_ORP_PE1_PH1__TSPAT_CV` SP
 ON A719.SPART = SP.SPART AND SP.SPRAS='E'
 JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__KONP_CV` KONP
 ON A719.KNUMH=KONP.KNUMH )a;

 -- Perform an UPSERT on the target table
-- Step 1: Identify records to be inserted

CREATE OR REPLACE TEMPORARY TABLE CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_TEMP_INS
AS
SELECT
	TMP.KNUMA_AG_A719
	,TMP.VKORG_A719
	,TMP.VTEXT_TVKOT
	,TMP.VTWEG_A719
	,TMP.VTEXT_TVTWT
	,TMP.SPART_A719
	,TMP.VTEXT_TSPAT
	,TMP.KUNAG_A719
	,TMP.IRM_PCNUM_A719
	,'Y' AS CURR_VRSN_FLG
	,TMP.LIFNR_A719
	,TMP.DATBI_A719
	,TMP.DATAB_A719
	,TMP.ROW_ADD_STP ,
	TMP.ROW_ADD_USER_ID,
	TMP.ROW_UPDATE_STP,
	TMP.ROW_UPDATE_USER_ID,
  TMP.LOEVM_KO_KONP
FROM CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_TEMP TMP
LEFT JOIN `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF` A719
ON TMP.KNUMA_AG_A719 = A719.KNUMA_AG_A719 AND TMP.SPART_A719 = A719.SPART_A719 AND TMP.KUNAG_A719 = A719.KUNAG_A719 AND TMP.IRM_PCNUM_A719 = A719.IRM_PCNUM_A719
AND TMP.LIFNR_A719 = A719.LIFNR_A719 AND TMP.DATBI_A719 = A719.DATBI_A719 AND TMP.DATAB_A719 = A719.DATAB_A719
WHERE
A719.KNUMA_AG_A719 IS NULL AND A719.SPART_A719  IS NULL AND A719.KUNAG_A719  IS NULL AND A719.IRM_PCNUM_A719  IS NULL AND A719.LIFNR_A719  IS NULL AND A719.DATBI_A719  IS NULL AND
A719.DATAB_A719  IS NULL;

-- Step 2: Update records where the source and target match

UPDATE `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF` A719
SET
	A719.KNUMA_AG_A719=TMP.KNUMA_AG_A719
	,A719.VKORG_A719=TMP.VKORG_A719
	,A719.VTEXT_TVKOT=TMP.VTEXT_TVKOT
	,A719.VTWEG_A719=TMP.VTWEG_A719
	,A719.VTEXT_TVTWT=TMP.VTEXT_TVTWT
	,A719.SPART_A719=TMP.SPART_A719
	,A719.VTEXT_TSPAT=TMP.VTEXT_TSPAT
	,A719.KUNAG_A719=TMP.KUNAG_A719
	,A719.IRM_PCNUM_A719=TMP.IRM_PCNUM_A719
	,A719.LIFNR_A719=TMP.LIFNR_A719
	,A719.DATBI_A719=TMP.DATBI_A719
	,A719.DATAB_A719=TMP.DATAB_A719
	,A719.ROW_UPDATE_USER_ID= v_stored_proc_name
FROM CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_TEMP TMP
WHERE TMP.KNUMA_AG_A719 = A719.KNUMA_AG_A719 AND TMP.SPART_A719 = A719.SPART_A719 AND TMP.KUNAG_A719 = A719.KUNAG_A719 AND TMP.IRM_PCNUM_A719 = A719.IRM_PCNUM_A719
AND TMP.LIFNR_A719 = A719.LIFNR_A719 AND TMP.DATBI_A719 = A719.DATBI_A719 AND TMP.DATAB_A719 = A719.DATAB_A719 ;

-- Step 3: Insert records into the target

INSERT INTO `PROJECT_ID.D1_PHM_CONFFACT_PRICING.CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF`
SELECT
	KNUMA_AG_A719
	,VKORG_A719
  ,VTEXT_TVKOT
	,VTWEG_A719
  ,VTEXT_TVTWT
	,SPART_A719
  ,VTEXT_TSPAT
	,KUNAG_A719
	,IRM_PCNUM_A719
	,LIFNR_A719
	,DATAB_A719
	,DATBI_A719
	,CURR_VRSN_FLG
	,ROW_ADD_STP
	,ROW_ADD_USER_ID
	,ROW_UPDATE_STP
	,ROW_UPDATE_USER_ID
  ,LOEVM_KO_KONP
FROM CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF_TEMP_INS;

END;