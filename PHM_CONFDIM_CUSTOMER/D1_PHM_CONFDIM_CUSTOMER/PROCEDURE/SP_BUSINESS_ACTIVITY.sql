CREATE PROCEDURE `PROJECT_ID`.D1_PHM_CONFDIM_CUSTOMER.SP_BUSINESS_ACTIVITY()
BEGIN

/***********************************************************************************************************************
Author: Rajesh Kumar P
Creation Date: 11/7/2023

Change History    [DATE]      [CHANGED BY]  [JIRA#]   [CODE REVIEW BY]    [DESCRIPTION]
<#>         <MM/DD/YYYY>  <Name>      <ID>

************************************************************************************************************************/

DECLARE v_start_time datetime;
DECLARE v_start_stp timestamp;
DECLARE v_end_stp timestamp;
DECLARE v_stored_proc_name STRING;

SET v_start_time = current_datetime();
SET v_start_stp = CURRENT_TIMESTAMP;
SET v_end_stp = (SELECT CAST('9999-12-31 23:59:59' AS TIMESTAMP));
SET v_stored_proc_name = 'D1_PHM_CONFDIM_CUSTOMER.SP_BUSINESS_ACTIVITY';

TRUNCATE TABLE `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.BUSINESS_ACTIVITY` ;

INSERT INTO `PROJECT_ID.D1_PHM_CONFDIM_CUSTOMER.BUSINESS_ACTIVITY` VALUES
 ('A','0','Retail Pharmacy'),
 ('A','1','Central Fill Pharmacy'),
 ('A','3','Chain Pharmacy'),
 ('A','4','Automated Dispensing System'),
 ('A','5','Online Retail Pharmacy'),
 ('A','6','Online Central Fill Pharmacy'),
 ('A','7','Online Chain Pharmacy'),
 ('A','8','Pharmacy-Mil'),
 ('A','9','Pharmacy-Fed'),
 ('B','0','Hospital/Clinic'),
 ('B','1','Chain Hospital/Clinic'),
 ('B','2','Hospital/Clinic-Military'),
 ('B','3','Hospital/Clinic-Federal'),
 ('B','4','Hospital/Clinic NG [National Guard]'),
 ('C','0','Practitioner'),
 ('C','1','Practitioner-DW/30'),
 ('C','2','Practitioner-Military'),
 ('C','3','MLP-Military'),
 ('C','4','Practitioner-DW/100'),
 ('C','5','Military Practitioner-DW/30'),
 ('C','6','Military Practitioner-DW/100'),
 ('C','7','Practitioner-DOD Contractor'),
 ('C','8','MLP-DOD Contractor'),
 ('C','9','Practitioner-DOD Contractor DW/30'),
 ('C','A','Practitioner-DOD Contractor DW/100'),
 ('C','B','Practitioner-DW/275'),
 ('C','C','Military Practitioner-DW/275'),
 ('C','D','Practitioner-DOD Contractor DW/275'),
 ('D','0','Teaching Institution'),
 ('E','0','Manufacturer'),
 ('E','1','Manufacturer (Bulk)'),
 ('F','0','Distributor'),
 ('F','1','Chempack/SNS Distributor'),
 ('G','0','Researcher (II-V)'),
 ('G','1','Canine Handler'),
 ('G','2','Researcher (I)'),
 ('H','0','Analytical Lab'),
 ('J','0','Importer'),
 ('J','1','Importer (C I, II)'),
 ('K','0','Exporter'),
 ('L','0','Reverse Distributor'),
 ('M','1','MLP-Ambulance Service'),
 ('M','2','MLP-Animal Shelter'),
 ('M','3','MLP-DR of Oriental Medicine'),
 ('M','4','MLP-Dept. of State'),
 ('M','5','MLP-Euthanasia Technician'),
 ('M','6','MLP-Homeopathic Physician'),
 ('M','7','MLP-Medical Psychologist'),
 ('M','8','MLP-Naturopathic Physician'),
 ('M','9','MLP-Nursing Home'),
 ('M','A','MLP-Nurse Practitioner'),
 ('M','B','MLP-Optometrist'),
 ('M','C','MLP-Physician Assistant'),
 ('M','D','MLP-Registered Pharmacist'),
 ('M','E','MLP-Certified Chiropractor'),
 ('N','0','Maintenance'),
 ('P','0','Detoxification'),
 ('R','0','Maintenance & Detox'),
 ('S','0','Compound/Maintenance'),
 ('T','0','Compound/Detoxification'),
 ('U','0','Comp/Maint/Detox'),
 ('W','0','Chemical Manufacturer'),
 ('X','0','Chemical Importer'),
 ('Y','0','Chemical Distributor'),
 ('Z','0','Chemical Exporter');

END;