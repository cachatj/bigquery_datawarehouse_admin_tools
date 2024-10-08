CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_DEA_REGISTRATION_INITIAL_LOAD()
BEGIN

/***********************************************************************************************************************
Author: Rajesh Kumar P
Creation Date: 11/7/2023
Data Sources: VI2_PHM_CONFDIM_CUSTOMER.PHM_BOT_DEA_RGSTRN_FEED_COMMON__DEA_REGISTRATION_CV
Change History    [DATE]      [CHANGED BY]  [JIRA#]   [CODE REVIEW BY]    [DESCRIPTION]
<#>         <MM/DD/YYYY>  <Name>      <ID>
************************************************************************************************************************/

DECLARE v_start_time datetime;
DECLARE v_start_stp timestamp;
DECLARE v_end_stp timestamp;
DECLARE v_stored_proc_name STRING;
--DECLARE LAST_RUN_STP TIMESTAMP;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_DEA_REGISTRATION_INITIAL_LOAD';
--SET LAST_RUN_STP =(SELECT MAX(ROW_ADD_STP) FROM `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_SDW__DEA_REGISTRATION_CV`);

TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.DEA_REGISTRATION_CV`;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.DEA_REGISTRATION_CV`
SELECT DISTINCT  * EXCEPT(RANK) FROM (
SELECT DEA_NUM, CURR_VRSN_FLG,DEL_FLG, VRSN_START_DTE, VRSN_END_DTE, PYMT_IND_ID, PYMT_IND_DESC, BSNS_ACT_ID, BSNS_ACT_SUB_ID, BSNS_ACT_DESC, DEA_RGSTRN_EXP_DTE, CMPNY_NAM, ADDL_CMPNY_INFO_TXT, ADDR_LINE_1_TXT, ADDR_LINE_2_TXT, CITY, STATE, DEA_SCHED_1_FLG, DEA_SCHED_2_FLG, DEA_SCHED_3_FLG, DEA_SCHED_2N_FLG, DEA_SCHED_3N_FLG, DEA_SCHED_4_FLG, DEA_SCHED_5_FLG, DEA_SCHED_L1_FLG, PSTL_CDE, DEGREE, STATE_LICENSE_NUM, STATE_CS_LICENSE_NUM, ROW_ADD_STP, ROW_ADD_USER_ID, ROW_UPDATE_STP, ROW_UPDATE_USER_ID,RANK () OVER (PARTITION BY DEA_NUM ORDER BY ROW_ADD_STP DESC) RANK FROM  `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_SDW__DEA_REGISTRATION_CV` WHERE CURR_VRSN_FLG ='Y') WHERE RANK = 1
UNION ALL
SELECT DISTINCT DEA_NUM, CURR_VRSN_FLG,DEL_FLG, VRSN_START_DTE, VRSN_END_DTE, PYMT_IND_ID, PYMT_IND_DESC, BSNS_ACT_ID, BSNS_ACT_SUB_ID, BSNS_ACT_DESC, DEA_RGSTRN_EXP_DTE, CMPNY_NAM, ADDL_CMPNY_INFO_TXT, ADDR_LINE_1_TXT, ADDR_LINE_2_TXT, CITY, STATE, DEA_SCHED_1_FLG, DEA_SCHED_2_FLG, DEA_SCHED_3_FLG, DEA_SCHED_2N_FLG, DEA_SCHED_3N_FLG, DEA_SCHED_4_FLG, DEA_SCHED_5_FLG, DEA_SCHED_L1_FLG, PSTL_CDE, DEGREE, STATE_LICENSE_NUM, STATE_CS_LICENSE_NUM, ROW_ADD_STP, ROW_ADD_USER_ID, ROW_UPDATE_STP, ROW_UPDATE_USER_ID FROM  `PROJECT_ID.VI2_PHM_CONFDIM_CUSTOMER.PHM_SDW__DEA_REGISTRATION_CV` WHERE  CURR_VRSN_FLG='N';

-- TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.DEA_REGISTRATION_EV`;

-- INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.DEA_REGISTRATION_EV`
-- VALUES (LAST_RUN_STP);

END;