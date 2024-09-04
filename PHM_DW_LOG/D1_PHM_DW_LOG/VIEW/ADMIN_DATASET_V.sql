CREATE VIEW `PROJECT_ID.D1_PHM_DW_LOG.ADMIN_DATASET_V`
AS SELECT
    current_date() AS snapshot_date,
    current_timestamp() AS snapshot_timestamp,
    *,
    REGEXP_REPLACE(SCHEMA_NAME, r'^[^_]+_','') AS schema_name_group,
    CASE
        WHEN SCHEMA_NAME LIKE 'VI2_%' THEN 'VI2'
        WHEN SCHEMA_NAME LIKE 'D1_%' THEN 'D1'
        WHEN SCHEMA_NAME LIKE 'D2_%' THEN 'D2'
        WHEN SCHEMA_NAME LIKE 'D4_%' THEN 'D4'
        WHEN SCHEMA_NAME LIKE 'VW_%' THEN 'VW'
        ELSE 'UNK'
    END AS dataset_group,
    'a-bd-'||REPLACE(LOWER(REGEXP_REPLACE(SCHEMA_NAME, r'^[^_]+_','')),'_','-')||'-nprd' AS dataset_group_ad_np,
    'a-bd-'||REPLACE(LOWER(REGEXP_REPLACE(SCHEMA_NAME, r'^[^_]+_','')),'_','-')||'-prd' AS dataset_group_ad_prod,
    CASE
        WHEN (
               SCHEMA_NAME LIKE '%_PHM_CONFDIM_%'
            OR SCHEMA_NAME LIKE '%_PHM_CONFFACT_%'
            OR SCHEMA_NAME LIKE '%_PHM_PR_SALES_CUBE%'
            OR SCHEMA_NAME LIKE '%_PHM_PD_GWSA_DMART%'
            OR SCHEMA_NAME LIKE '%_PHM_PD_GWSA_OPS' -- new gwsa dataset, stand alone without vi2
            OR SCHEMA_NAME LIKE '%_PHM_PD_DSCSA_EPCIS%' -- still under development
            OR SCHEMA_NAME LIKE '%_PHM_PD_CSL_RPT' -- stillunder development
            OR SCHEMA_NAME LIKE '%D1_PHM_DW_LOG'
            OR SCHEMA_NAME LIKE '%_PHM_HDG_SALE_FACT'
            OR SCHEMA_NAME LIKE '%_PHM_CONFFACT_CSMP'
            OR SCHEMA_NAME LIKE '%_PHM_PD_CONFFACT_PRICING'
            OR SCHEMA_NAME LIKE '%_PHM_PD_ECONDISC_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_PRTRCO_DEADNET_RPT'-- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_HLTH_TRUST_INV_RPT'-- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_ICONTRACTS_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_KROGER_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_REDOAK_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_VNDV_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%PHM_PD_PRTRCO_TRNSFR_PRICE_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_TRULLA_INV_RPT' -- etl dataset
            OR SCHEMA_NAME LIKE '%_PHM_PD_OE_CONV' -- Order Express
            OR SCHEMA_NAME LIKE '%_PHM_PD_OE_SAS_CONV'
            OR SCHEMA_NAME LIKE '%_PHM_PD_SUPPLI_DVRS' -- Supplier Diversity
            OR SCHEMA_NAME LIKE '%PHM_PD_SP_GPO_CGBK_DSHBRD' -- GPO scorecard
            OR SCHEMA_NAME LIKE '%PHM_PD_FUSE_RETAIL' -- Fuse Retail
            OR SCHEMA_NAME LIKE '%PHM_OPRTN_MATERIAL' -- ekram
            OR SCHEMA_NAME LIKE '%PHM_OPRTN_CUSTOMER' -- ekram
            OR SCHEMA_NAME LIKE '%PHM_PD_PDW_CONS_HLTH' -- Consumer Health (Adding to PDW Looker Project)
            OR SCHEMA_NAME LIKE '%PHM_PD_TDW_HIST' -- TDR Historical Data
        ) THEN TRUE
        ELSE FALSE
    END AS pdw_flag,
    REGEXP_EXTRACT(ddl, r'friendly_name="([^"]*)"') AS friendly_name,
    REGEXP_EXTRACT(ddl, r'description="([^"]*)"') AS description,
    --REGEXP_EXTRACT(ddl, r'location="([^"]*)"') AS location,
    REGEXP_EXTRACT(ddl, r'STRUCT\("apmid", "([^"]*)"\)') AS apmid,
    REGEXP_EXTRACT(ddl, r'STRUCT\("cost-center", "([^"]*)"\)') AS cost_center,
    REGEXP_EXTRACT(ddl, r'STRUCT\("data-owner", "([^"]*)"\)') AS data_owner,
    REGEXP_EXTRACT(ddl, r'STRUCT\("data-requestor", "([^"]*)"\)') AS data_requestor,
    REGEXP_EXTRACT(ddl, r'STRUCT\("datasetname", "([^"]*)"\)') AS datasetname,
    REGEXP_EXTRACT(ddl, r'STRUCT\("environment", "([^"]*)"\)') AS environment,
    REGEXP_EXTRACT(ddl, r'STRUCT\("non-regulated-class", "([^"]*)"\)') AS non_regulated_class,
    REGEXP_EXTRACT(ddl, r'STRUCT\("privacy-class", "([^"]*)"\)') AS privacy_class,
    REGEXP_EXTRACT(ddl, r'STRUCT\("privacy-flag", "([^"]*)"\)') AS privacy_flag,

FROM `PROJECT_ID.region-us`.INFORMATION_SCHEMA.SCHEMATA;