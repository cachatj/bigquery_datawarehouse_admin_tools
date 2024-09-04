CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_PLANT_RLT()
BEGIN

/***********************************************************************************************************************
Author: Debapriya Banerjee
Creation Date: 09/29/2023
Data Sources:	PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MARC_CV
				PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TMVFT_CV
				PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T024_CV

*************************************************************************************************************************
Change History		[DATE]			[CHANGED BY]	[JIRA#]		[CODE REVIEW BY]		[DESCRIPTION]
	<#>					<MM/DD/YYYY>	<Name>			<ID>

************************************************************************************************************************/

DECLARE v_start_time datetime;
DECLARE v_start_stp timestamp;
DECLARE v_end_stp timestamp;
DECLARE v_stored_proc_name string;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_PLANT_RLT';

--Get minimum date(ZSLTSTMP) from marc table
CREATE OR REPLACE TEMPORARY TABLE GET_ZSLTSTMP_DTE
AS
SELECT WERKS, MATNR, MIN(DATE(PARSE_DATETIME('%Y%m%d%H%M%S', ZSLTSTMP))) AS zsltstmp_marc_stp
  FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MARC_HV`
GROUP BY 1, 2
;

-- MATERIAL_PLANT_RLT will be truncated and loaded daily with the rest of the Material objects

-- Truncate MATERIAL_PLANT_RLT

TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_PLANT_RLT_CV`;

-- Insert into MATERIAL_PLANT_RLT

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_PLANT_RLT_CV`
SELECT
	MARC.MATNR        															AS MATNR_MARC,
	MARC.WERKS        															AS WERKS_MARC,
	MARC.PSTAT        															AS PSTAT_MARC,
	MARC.LVORM        															AS LVORM_MARC,
	MARC.XCHAR        															AS XCHAR_MARC,
	MARC.MMSTA        															AS MMSTA_MARC,
	IFNULL(SAFE_CAST(MARC.MMSTD AS DATE FORMAT 'YYYYMMDD'), DATE '1900-01-01') AS MMSTD_MARC,
	MARC.EKGRP        															AS EKGRP_MARC,
	IFNULL(T024.EKNAM, 'NOT_AVAILABLE')							AS EKNAM_T024,
	MARC.DISPR        															AS DISPR_MARC,
	MARC.DISMM        															AS DISMM_MARC,
	MARC.XCHPF        															AS XCHPF_MARC,
	MARC.MTVFP        															AS MTVFP_MARC,
	IFNULL(TMVFT.BEZEI, 'NOT_AVAILABLE')						AS BEZEI_TMVFT,
	MARC.PRCTR        															AS PRCTR_MARC,
	MARC.ABCIN        															AS ABCIN_MARC,
	MARC.SERNP        															AS SERNP_MARC,
	MARC.XMCNG        															AS XMCNG_MARC,
	MARC.CCFIX        															AS CCFIX_MARC,
	MARC.MFRGR        															AS MFRGR_MARC,
	MARC.LOGGR        															AS LOGGR_MARC,
	MARC.FPRFM        															AS FPRFM_MARC,
	SAFE_CAST(MARC.LFMON AS INT64)									AS LFMON_MARC,
	SAFE_CAST(MARC.LFGJA AS INT64)									AS LFGJA_MARC,
	MARC.BWESB        															AS BWESB_MARC,
	MARC.YYNOINVENTORY															AS YYNOINVENTORY_MARC,
	MARC.YYCALLTO     															AS YYCALLTO_MARC,
	MARC.YYLOTM       															AS YYLOTM_MARC,
	IFNULL(ZSL.zsltstmp_marc_stp,DATE(PARSE_DATETIME('%Y%m%d%H%M%S', MARC.ZSLTSTMP)))	AS ZSLTSTMP_MARC,
	v_start_stp 																		AS ROW_ADD_STP,
	v_stored_proc_name															AS ROW_ADD_USER_ID,
	v_start_stp 																		AS ROW_UPDATE_STP,
	v_stored_proc_name															AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MARC_CV` MARC
LEFT JOIN GET_ZSLTSTMP_DTE ZSL ON MARC.MATNR = ZSL.MATNR AND MARC.WERKS = ZSL.WERKS
	LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TMVFT_CV` TMVFT ON MARC.MTVFP = TMVFT.MTVFP AND TMVFT.SPRAS = 'E'
	LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T024_CV` T024 ON MARC.EKGRP = T024.EKGRP

;

END;