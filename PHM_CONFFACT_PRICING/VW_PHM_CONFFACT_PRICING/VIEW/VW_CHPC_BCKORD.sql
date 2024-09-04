CREATE VIEW `PROJECT_ID.VW_PHM_CONFFACT_PRICING.VW_CHPC_BCKORD`(
  cardinal_substitution_key,
  vendor_name,
  corporate_item_number,
  national_drug_code,
  corporate_description,
  AAAITEM,
  availability_alert_code,
  external_message_code,
  external_message,
  expected_delivery_date,
  internal_message,
  change_date,
  change_time,
  origination_date,
  expediting_code_type,
  record_effective_date
)
AS SELECT
cardinal_substitution_key
,vendor_name
,corporate_item_number
,national_drug_code
,corporate_description
,AAAITEM
,availability_alert_code
,external_message_code
,external_message
,expected_delivery_date
,internal_message
,change_date
,change_time
,origination_date
,expediting_code_type
,record_effective_date

 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_BCKORD`;