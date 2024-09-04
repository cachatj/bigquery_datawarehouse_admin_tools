CREATE VIEW `PROJECT_ID.VW_PHM_CONFFACT_PRICING.VW_CHPC_MBR_ACCESS`(
  root_affiliation_number,
  root_affiliation_name,
  affiliation_number,
  affiliation_name,
  group_id,
  ship_to_customer_number,
  ship_to_customer_name,
  group_number,
  group_name,
  group_priority_ranking,
  secondary_priority_ranking,
  account_status_flag,
  relationship_status_flag,
  relationship_effective_date,
  edi_type_code,
  record_effective_date,
  A2_root_affiliation_number,
  A2_root_affiliation_name
)
AS SELECT
root_affiliation_number
,root_affiliation_name
,affiliation_number
,affiliation_name
,group_id
,ship_to_customer_number
,ship_to_customer_name
,group_number
,group_name
,group_priority_ranking
,secondary_priority_ranking
,account_status_flag
,relationship_status_flag
,relationship_effective_date
,edi_type_code
,record_effective_date
,A2_root_affiliation_number
,A2_root_affiliation_name

 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.CHPC_MBR_ACCESS`;