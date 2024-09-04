CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_MATERIAL.SP_LAST_TIMESTAMP_MATERIAL_EV()
BEGIN

/***********************************************************************************************************************
* Manual SQL insert
***********************************************************************************************************************/

INSERT INTO  `PROJECT_ID.D1_PHM_CONFDIM_MATERIAL.LAST_TIMESTAMP_MATERIAL_EV`
(SP_MATERIAL_DELTA_LOAD_STP,
SP_MATERIAL_PRICING_VF_DELTA_LOAD_STP
)
VALUES
('1900-01-01 00:00:00 UTC','1900-01-01 00:00:00 UTC')
;
END;