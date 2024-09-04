with 
main as(

SELECT DISTINCT
  
  entities.D0_Dataset,
  entities.D0_Entity,
  entities.VI2_Dataset,
  entities.VI2_Entity,
  COLUMNS_vi2.column_name as column_name_vi2,
  COLUMNS_vi2.column_name ||"_"||REPLACE(entities.D0_Entity,"_CV","") as test_column_name,
  COLUMNS_d1.column_name as column_name_d1,
  entities.D1_Dataset,
  entities.D1_Entity,
  -- entities.VW_Dataset,
  -- entities.VW_Entity,
  

FROM
  `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_ENTITY_LINEAGE_V` entities

--Bring in list of all columns in the VI2 table

LEFT JOIN
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_vi2
  ON entities.VI2_Dataset = columns_vi2.table_schema
  AND entities.VI2_Entity = columns_vi2.table_name

--Bring in list of all columns in the d1 table

 JOIN
  `PROJECT_ID`.D1_PHM_DW_LOG.ADMIN_COLUMNS_T columns_d1
  ON entities.D1_Dataset = columns_d1.table_schema
  AND entities.D1_Entity = columns_d1.table_name

  --AND COLUMNS_vi2.column_name ||"_"||REPLACE(entities.D0_Entity,"_CV","") =  columns_d1.column_name





WHERE
  D1_Entity LIKE '%CUSTOMER_SALES_AREA_RLT%'
  AND COLUMNS_vi2.column_name not like '%D0_%'
)

select * from main
where 1=1
AND test_column_name LIKE CONCAT(column_name_d1, '%') 
  OR test_column_name LIKE CONCAT(column_name_d1, '%')



