CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_LOOKER_METADATA_ENTITY_V`
AS WITH entities AS (
    SELECT
        DISTINCT LOWER(table_catalog) AS table_catalog,
        LOWER(table_schema) AS table_schema,
        LOWER(TABLE_NAME) AS TABLE_NAME,
        CASE
            -- exception mapping for when table names don't align

            WHEN TABLE_NAME = 'SHIPTO_CUSTOMER_SALES_AREA_RLT_CV' THEN 'CUSTOMER_SALES_AREA_RLT'

            WHEN TABLE_NAME = 'TIME_SMRY_LAST_30_MONTH_CV' THEN 'TIME_SMRY'

            ELSE REGEXP_REPLACE(TABLE_NAME, '_CV$', '')

        EPROJECT_ID_cleaned,

        LOWER(table_type) AS table_type,

        SAFE_CAST(creation_time AS DATE) AS table_create_date,

    FROM

        `edna-data-np-cah.D1_PHM_DW_LOG.ADMIN_TABLES_COLUMNS`

    WHERE

        1 = 1

        AND table_schema LIKE '%_PHM_CONFDIM_%'

        OR table_schema LIKE '%_PHM_CONFFACT_%'

        OR table_schema LIKE '%_PHM_PR_SALES_CUBE%'

        OR table_schema LIKE '%_PHM_PD_GWSA_DMART%'

        OR table_schema LIKE '%_PHM_PD_DSCSA_EPCIS%' -- still under development

        OR table_schema LIKE '%_PHM_PD_DEA_RGSTRN' -- stillunder development

),

consumption_entities AS (

    SELECT

        DISTINCT table_schema,

        TABLE_NAME,

        table_name_cleaned,

        table_type,

        table_create_date,

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

    vw.table_create_date AS vw_create_date,

    d1.table_create_date AS d1_create_date,

    d1.table_schema AS d1_schema,

    d1.table_name AS d1_entity_name,

    d1.table_type AS d1_entity_type,



FROM

    consumption_entities vw

    LEFT JOIN entities d1 ON vw.table_name_cleaned = d1.table_name_cleaned --AND vw.ordinal_position = d1.ordinal_position

    AND d1.table_schema LIKE '%d1_%'
WHERE
    1 = 1
    AND d1.table_schema IS NOT NULL
ORDER BY
    1,
    2 ASC;