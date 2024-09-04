CREATE VIEW `PROJECT_ID.VW_PHM_CONFFACT_PRICING.VW_ITEM_BLOCK`(
  division_number,
  ship_to_customer_number,
  group_number,
  corporate_item_number,
  relationship_status,
  relationship_effective_date,
  record_effective_date,
  Contract_Number
)
AS SELECT
division_number
,ship_to_customer_number
,group_number
,corporate_item_number
,relationship_status
,relationship_effective_date
,record_effective_date
,Contract_Number

 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.ITEM_BLOCK`;