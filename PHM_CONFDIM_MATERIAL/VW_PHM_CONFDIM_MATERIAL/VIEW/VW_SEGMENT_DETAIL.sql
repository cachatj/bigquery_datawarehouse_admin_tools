CREATE VIEW `PROJECT_ID.VW_PHM_CONFDIM_MATERIAL.VW_SEGMENT_DETAIL`(
  corporate_item_number,
  subgroup_number,
  effective_date,
  term_date,
  record_effective_date
)
AS SELECT
corporate_item_number
,subgroup_number
,effective_date
,term_date
,record_effective_date
 FROM `PROJECT_ID.D2_PHM_CONFFACT_PRICING.SEGMENT_DETAIL`;