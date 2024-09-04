CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.VW_SEGMENT_MASTER`(
  subgroup_number,
  subgroup_description,
  group_number,
  subgroup_effective_date,
  subgroup_term_date,
  record_effective_date
)
AS SELECT
subgroup_number
,subgroup_description
,group_number
,subgroup_effective_date
,subgroup_term_date
,record_effective_date

 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SEGMENT_MASTER`;