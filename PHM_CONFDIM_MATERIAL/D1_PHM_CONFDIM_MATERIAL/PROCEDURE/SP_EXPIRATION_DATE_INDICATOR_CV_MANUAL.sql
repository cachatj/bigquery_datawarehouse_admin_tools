CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_EXPIRATION_DATE_INDICATOR_CV_MANUAL()
BEGIN

/***********************************************************************************************************************

* Manual SQL insert

***********************************************************************************************************************/

--Insert into EXPIRATION_DATE_INDICATOR_CV table (MATERIAL MODEL)
INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.EXPIRATION_DATE_INDICATOR_CV`
VALUES ('B','EXPIRATION DATE',current_timestamp(),'EXPIRATION_DATE_INDICATOR',current_timestamp(),'EXPIRATION_DATE_INDICATOR'),
('E','SHELF LIFE EXPIRATION DATE',current_timestamp(),'EXPIRATION_DATE_INDICATOR',current_timestamp(),'EXPIRATION_DATE_INDICATOR')
;

END;