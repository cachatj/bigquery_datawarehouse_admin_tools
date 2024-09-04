CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_DISPLAY()
BEGIN

/***********************************************************************************************************************
Author: Debapriya Banerjee
Creation Date: 09/29/2023
Data Sources:	PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MAST_CV
				PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MAKT_CV
				PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__STPO_CV
				PROJECT_ID.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T006A_CV
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
SET v_stored_proc_name = 'D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_DISPLAY';

-- MATERIAL_DISPLAY_CV will be truncated and loaded daily with the rest of the Material objects

-- Truncate MATERIAL_DISPLAY_CV

TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.MATERIAL_DISPLAY_CV`;

-- Insert into MATERIAL_DISPLAY_CV
-- There's a DISTINCT clause on the SELECT since MAST is keyed by MATNR & WERKS. However, the Bill of Materials (STLNR) is the same for all plants (WERKS)

PROJECT_ID

INSERT INTO `edna-data-np-cah.D1_PHM_CONFDIM_MATERIAL.MATERIAL_DISPLAY_CV`

SELECT DISTINCT

   MAST.MATNR								AS MATNR_MAST,

   IFNULL(MAKT_MAT.MAKTX, 'NOT_AVAILABLE')	AS MAKTX_MAKT,

   STPO.IDNRK								AS IDNRK_STPO,

   IFNULL(MAKT_CMP.MAKTX, 'NOT_AVAILABLE')	AS MAKTX_MAKT_COMP,
PROJECT_ID
   STPO.MENGEPROJECT_ID_STPO,
PROJECT_ID
   STPO.MEINPROJECT_IDS_STPO,
PROJECT_ID
   IFNULL(T006A.MSEHL, 'NOT_AVAILABLE')		AS MSEHL_T006A,

   v_start_stp 								AS ROW_ADD_STP,

   v_stored_proc_name						AS ROW_ADD_USER_ID,

   v_start_stp 								AS ROW_UPDATE_STP,

   v_stored_proc_name						AS ROW_UPDATE_USER_ID

FROM `edna-data-np-cah.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MAST_CV` MAST

	INNER JOIN `edna-data-np-cah.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__STPO_CV` STPO ON MAST.STLNR =  STPO.STLNR

	LEFT JOIN `edna-data-np-cah.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MAKT_CV` MAKT_MAT ON MAST.MATNR = MAKT_MAT.MATNR AND MAKT_MAT.SPRAS = 'E'

	LEFT JOIN `edna-data-np-cah.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__MAKT_CV` MAKT_CMP ON STPO.IDNRK = MAKT_CMP.MATNR AND MAKT_CMP.SPRAS = 'E'

	LEFT JOIN `edna-data-np-cah.VI2_PHM_CONFDIM_MATERIAL.PHM_ORP_PE1_PH1__T006A_CV` T006A ON STPO.MEINS = T006A.MSEHI AND T006A.SPRAS = 'E'
;

END;