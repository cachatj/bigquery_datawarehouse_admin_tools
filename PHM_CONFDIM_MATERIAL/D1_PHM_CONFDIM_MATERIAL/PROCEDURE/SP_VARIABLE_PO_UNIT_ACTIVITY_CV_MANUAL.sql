CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_VARIABLE_PO_UNIT_ACTIVITY_CV_MANUAL()
BEGIN

/***********************************************************************************************************************

* Manual SQL insert

***********************************************************************************************************************/

--Insert into VARIABLE_PO_UNIT_ACTIVITY_CV table (MATERIAL MODEL)
INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.VARIABLE_PO_UNIT_ACTIVITY_CV`
VALUES (' ','NOT ACTIVE',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY'),
('2','ACTIVE WITH OWN PRICE',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY'),
('1','ACTIVE',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY',current_timestamp(),'VARIABLE_PO_UNIT_ACTIVITY')
;

END;