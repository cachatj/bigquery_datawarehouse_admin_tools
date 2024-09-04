CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_COLUMNS_V`
AS WITH
entities
AS (
    SELECT
        LOWER(table_catalog) AS table_catalog,
        LOWER(table_schema) AS table_schema,
        LOWER(TABLE_NAME) AS TABLE_NAME,
        CASE
            -- exception mapping for when table names don't align



            WHEN TABLE_NAME = 'SHIPTO_CUSTOMER_SALES_AREA_RLT_CV' THEN 'CUSTOMER_SALES_AREA_RLT'



            WHEN TABLE_NAME = 'TIME_SMRY_LAST_30_MONTH_CV' THEN 'TIME_SMRY'



            ELSE REGEXP_REPLACE(TABLE_NAME, '_CV$', '')



        END AS table_name_cleaned,



        LOWER(table_type) AS table_type,



        LOWER(COLUMN_NAME) AS COLUMN_NAME,



        description,



        ordinal_position,



        LOWER(data_type) AS data_type,



    FROM



        `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS`



    WHERE



        1 = 1



        --- Filtering out datasets that aren't used in the looker semantic layer but are still under tdr ownership
        AND table_schema NOT LIKE '%_PHM_PD_ECONDISC_RPT' -- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_PRTRCO_DEADNET_RPT'-- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_HLTH_TRUST_INV_RPT'-- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_ICONTRACTS_RPT' -- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_KROGER_RPT' -- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_REDOAK_RPT' -- etl dataset
        AND table_schema NOT LIKE '%_PHM_PD_TRULLA_INV_RPT' -- etl dataset
),
consumption_entities AS (
    SELECT
        DISTINCT table_schema,
        TABLE_NAME,
        table_name_cleaned,
        table_type,
        COLUMN_NAME,
        description,
        ordinal_position,
        data_type,
    FROM
        entities
    WHERE
        table_schema LIKE '%vw_%'
)
SELECT
    DISTINCT vw.table_schema AS vw_schema,
    vw.table_name AS vw_entity_name,
    vw.table_name_cleaned AS vw_table_name_cleaned,
    d1.table_name_cleaned AS d1_table_name_cleaned,
    vw.table_type AS vw_entity_type,
    vw.column_name AS vw_column_name,
    vw.description AS vw_description,
    vw.ordinal_position AS vw_ordinal_postition,
    vw.data_type AS vw_data_type,
    d1.table_schema AS d1_schema,
    d1.table_name AS d1_entity_name,
    d1.table_type AS d1_entity_type,
    d1.column_name AS d1_column_name,
    d1.description AS d1_description,
    d1.ordinal_position AS d1_ordinal_postition,
    d1.data_type AS d1_data_type,
    CASE
        WHEN d1.data_type = 'string' THEN 'string'
        WHEN d1.data_type = 'numeric' THEN 'number'
        WHEN d1.data_type = 'timestamp' THEN 'time'
        WHEN d1.data_type = 'int64' THEN 'number'
        WHEN d1.data_type = 'date' THEN 'date'
        WHEN d1.data_type = 'float64' THEN 'number'
        WHEN d1.data_type = 'time' THEN 'time'
        WHEN d1.data_type = 'datetime' THEN 'time'
        WHEN d1.data_type = 'bytes' THEN 'number'
        WHEN d1.data_type = 'bignumeric' THEN 'number'
        WHEN d1.data_type = 'bool' THEN 'yesno'
        WHEN d1.data_type = 'bool' THEN 'yesno'
        ELSE 'unknown'
    END AS looker_data_type,
    vw.description AS looker_label,
    d1.description || ' (' || UPPER(d1.column_name) || ')' AS looker_description,
    '/views/base_views/' || vw.table_schema || '/' || vw.table_name || '.view.lkml' AS looker_base_view_path,
    '/views/refinement_views/' || vw.table_schema || '/' || vw.table_name || '.view.lkml' AS looker_refinement_view_path,
    '+' || vw.table_name AS looker_refinement_view_name,
    '`PROJECT_ID.' || UPPER(vw.table_schema) || '.' || UPPER(vw.table_name) || "`" AS looker_sql_table_name,
    "`{{_user_attributes['env']}}." || UPPER(vw.table_schema) || '.' || UPPER(vw.table_name) || "`" AS looker_sql_table_name_param,
    -- view: buying_group_customer_rlt_cv {
    --sql_table_name: `PROJECT_ID.VW_PHM_CONFDIM_CONTRACT.BUYING_GROUP_CUSTOMER_RLT_CV` ;;
FROM
    consumption_entities vw
    LEFT JOIN entities d1
     ON vw.table_name_cleaned = d1.table_name_cleaned
     AND vw.ordinal_position = d1.ordinal_position
   -- AND d1.table_schema LIKE '%d1_%'
    AND (d1.table_schema LIKE '%d1_%' OR UPPER(d1.table_schema) = 'D2_PHM_PD_PDW_CONS_HLTH' OR UPPER(d1.table_schema) = 'D2_PHM_PD_ECOM_ADV_RPT')
WHERE
    1 = 1
    AND d1.table_schema IS NOT NULL
ORDER BY
    1,
    2,
    6 ASC;