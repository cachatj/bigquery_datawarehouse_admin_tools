CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_SALES_AREA_RLT()
BEGIN

/***********************************************************************************************************************
Author: Debapriya Banerjee
Creation Date: 09/29/2023
Data Sources:	VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MVKE_CV
				VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TPTMT_CV
				VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TVMST_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_SALES_AREA_RLT';

-- MATERIAL_SALES_AREA_RLT will be truncated and loaded daily with the rest of the Material objects

-- Truncate MATERIAL_SALES_AREA_RLT

TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_SALES_AREA_RLT_CV`;

-- Insert into MATERIAL_SALES_AREA_RLT

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_SALES_AREA_RLT_CV`
SELECT
	MVKE.MATNR        								AS MATNR_MVKE ,
	MVKE.VKORG        								AS VKORG_MVKE ,
	MVKE.VTWEG        								AS VTWEG_MVKE ,
	MVKE.LVORM        								AS LVORM_MVKE ,
	MVKE.VMSTA        								AS VMSTA_MVKE ,
	IFNULL(TVMST.VMSTB, 'NOT_AVAILABLE')			AS VMSTB_TVMST,
	IFNULL(SAFE_CAST(MVKE.VMSTD AS DATE FORMAT 'YYYYMMDD'), DATE '1900-01-01') AS VMSTD_MVKE ,
	MVKE.MTPOS        								AS MTPOS_MVKE ,
	IFNULL(TPTMT.BEZEI, 'NOT_AVAILABLE')			AS BEZEI_TPTMT,
	MVKE.KONDM        								AS KONDM_MVKE ,
	MVKE.KTGRM        								AS KTGRM_MVKE ,
	MVKE.PRAT1        								AS PRAT1_MVKE ,
	MVKE.PRAT3        								AS PRAT3_MVKE ,
	MVKE.YYRMAT       								AS YYRMAT_MVKE,
	MVKE.YYCMAT       								AS YYCMAT_MVKE,
	MVKE.YYFMAT       								AS YYFMAT_MVKE,
	v_start_stp 									AS ROW_ADD_STP,
	v_stored_proc_name								AS ROW_ADD_USER_ID,
	v_start_stp 									AS ROW_UPDATE_STP,
	v_stored_proc_name								AS ROW_UPDATE_USER_ID
FROM `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MVKE_CV` MVKE
	LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TPTMT_CV` TPTMT ON MVKE.MTPOS = TPTMT.MTPOS AND TPTMT.SPRAS = 'E'
	LEFT JOIN `PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__TVMST_CV` TVMST ON MVKE.VMSTA = TVMST.VMSTA AND TVMST.SPRAS = 'E'
;

END;