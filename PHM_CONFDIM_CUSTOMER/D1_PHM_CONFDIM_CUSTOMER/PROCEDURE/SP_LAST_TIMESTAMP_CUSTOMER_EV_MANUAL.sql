CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_LAST_TIMESTAMP_CUSTOMER_EV_MANUAL()
BEGIN

/***********************************************************************************************************************
* Manual SQL insert
***********************************************************************************************************************/

INSERT INTO  `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.LAST_TIMESTAMP_CUSTOMER_EV` (SP_CUSTOMER_DELTA_LOAD_STP,SP_CUSTOMER_SALES_AREA_RLT_DELTA_LOAD_STP) VALUES('1900-01-01 00:00:00 UTC','1900-01-01 00:00:00 UTC');

END;