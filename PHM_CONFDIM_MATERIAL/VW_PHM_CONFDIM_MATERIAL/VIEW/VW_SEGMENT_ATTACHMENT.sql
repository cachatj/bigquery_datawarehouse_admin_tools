CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.VW_SEGMENT_ATTACHMENT`(
  division_number,
  ship_to_customer_number,
  subgroup_number,
  attachment_effective_date,
  attachment_term_date,
  percent_1,
  percent_2,
  group_number,
  record_effective_date
)
AS SELECT
division_number
,ship_to_customer_number
,subgroup_number
,attachment_effective_date
,attachment_term_date
,percent_1
,percent_2
,group_number
,record_effective_date
 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SEGMENT_ATTACHMENT`;