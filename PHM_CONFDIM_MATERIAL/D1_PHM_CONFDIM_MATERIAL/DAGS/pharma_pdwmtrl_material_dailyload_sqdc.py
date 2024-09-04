from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from custom_operators import ServiceNowAlertingOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'pdna',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': ['G-EIT-PharmaDnA-Run@cardinalhealth.com'],
}


# Fetch environment from Airflow Variables
airflow_env = Variable.get("env")
project_id = Variable.get("BQ_DATA_PROJECT")


# Initialize environment-specific variables
if airflow_env == "dev":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-np-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-np-cah'
    schedule_interval='015 06 * * 0'
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
    schedule_interval='015 06 * * 0'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    schedule_interval='015 06 * * *'


# DAG Definition
with DAG(
    'pharma_pdwmtrl_material_dailyload_sqdc',
    default_args=default_args,
    description=f'Process for updating pharma_pdwmtrl_material_dailyload_sqdc',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['pdw', 'material','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:

    execute_sp_rems_program = BigQueryInsertJobOperator(
        task_id='execute_sp_rems_program',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_REMS_PROGRAM();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_hazmat_storage = BigQueryInsertJobOperator(
        task_id='execute_sp_hazmat_storage',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_HAZMAT_STORAGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_cross_distrib_chain_mtrl_status = BigQueryInsertJobOperator(
        task_id='execute_sp_cross_distrib_chain_mtrl_status',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_CROSS_DISTRIB_CHAIN_MTRL_STATUS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_hamacher_finer_class = BigQueryInsertJobOperator(
        task_id='execute_sp_hamacher_finer_class',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_HAMACHER_FINER_CLASS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_dangerous_good_indicator = BigQueryInsertJobOperator(
        task_id='execute_sp_dangerous_good_indicator',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_DANGEROUS_GOOD_INDICATOR();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_hamacher_finest_class = BigQueryInsertJobOperator(
        task_id='execute_sp_hamacher_finest_class',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_HAMACHER_FINEST_CLASS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_pricing_usage = BigQueryInsertJobOperator(
        task_id='execute_sp_pricing_usage',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PRICING_USAGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_cross_plant_material_status = BigQueryInsertJobOperator(
        task_id='execute_sp_cross_plant_material_status',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_CROSS_PLANT_MATERIAL_STATUS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_list_1_chemical = BigQueryInsertJobOperator(
        task_id='execute_sp_list_1_chemical',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_LIST_1_CHEMICAL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_special_handling = BigQueryInsertJobOperator(
        task_id='execute_sp_special_handling',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_SPECIAL_HANDLING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_group = BigQueryInsertJobOperator(
        task_id='execute_sp_material_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_item_category_group = BigQueryInsertJobOperator(
        task_id='execute_sp_item_category_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_ITEM_CATEGORY_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_temperature_condition = BigQueryInsertJobOperator(
        task_id='execute_sp_temperature_condition',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_TEMPERATURE_CONDITION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_industry_sector = BigQueryInsertJobOperator(
        task_id='execute_sp_industry_sector',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_INDUSTRY_SECTOR();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_ean_category = BigQueryInsertJobOperator(
        task_id='execute_sp_ean_category',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_EAN_CATEGORY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_hamacher_fine_class = BigQueryInsertJobOperator(
        task_id='execute_sp_hamacher_fine_class',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_HAMACHER_FINE_CLASS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_package_reference = BigQueryInsertJobOperator(
        task_id='execute_sp_package_reference',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PACKAGE_REFERENCE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_form = BigQueryInsertJobOperator(
        task_id='execute_sp_form',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FORM();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_packaging_indicator = BigQueryInsertJobOperator(
        task_id='execute_sp_packaging_indicator',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PACKAGING_INDICATOR();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_storage_condition = BigQueryInsertJobOperator(
        task_id='execute_sp_storage_condition',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_STORAGE_CONDITION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_product_allocation = BigQueryInsertJobOperator(
        task_id='execute_sp_product_allocation',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PRODUCT_ALLOCATION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_transportation_group = BigQueryInsertJobOperator(
        task_id='execute_sp_transportation_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_TRANSPORTATION_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_packaging_material_type = BigQueryInsertJobOperator(
        task_id='execute_sp_packaging_material_type',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PACKAGING_MATERIAL_TYPE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_type = BigQueryInsertJobOperator(
        task_id='execute_sp_material_type',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_TYPE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_product_hierarchy = BigQueryInsertJobOperator(
        task_id='execute_sp_product_hierarchy',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_PRODUCT_HIERARCHY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_legal_control = BigQueryInsertJobOperator(
        task_id='execute_sp_legal_control',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_LEGAL_CONTROL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_dea_sub_base_code = BigQueryInsertJobOperator(
        task_id='execute_sp_dea_sub_base_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_DEA_SUB_BASE_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_dea_base_code = BigQueryInsertJobOperator(
        task_id='execute_sp_dea_base_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_DEA_BASE_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_sales_tax_driver = BigQueryInsertJobOperator(
        task_id='execute_sp_sales_tax_driver',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_SALES_TAX_DRIVER();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_ahfs_code = BigQueryInsertJobOperator(
        task_id='execute_sp_ahfs_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_AHFS_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_orange_book_code = BigQueryInsertJobOperator(
        task_id='execute_sp_orange_book_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_ORANGE_BOOK_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_unit_of_measure = BigQueryInsertJobOperator(
        task_id='execute_sp_unit_of_measure',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_UNIT_OF_MEASURE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_mtrl_grp_packaging_materials = BigQueryInsertJobOperator(
        task_id='execute_sp_mtrl_grp_packaging_materials',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MTRL_GRP_PACKAGING_MATERIALS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_class_rlt = BigQueryInsertJobOperator(
        task_id='execute_sp_material_class_rlt',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_CLASS_RLT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_display = BigQueryInsertJobOperator(
        task_id='execute_sp_material_display',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_DISPLAY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_sales_area_rlt = BigQueryInsertJobOperator(
        task_id='execute_sp_material_sales_area_rlt',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_SALES_AREA_RLT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_equivalence = BigQueryInsertJobOperator(
        task_id='execute_sp_material_equivalence',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_EQUIVALENCE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_plant_rlt = BigQueryInsertJobOperator(
        task_id='execute_sp_material_plant_rlt',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_PLANT_RLT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_retail_tier_price_vf = BigQueryInsertJobOperator(
        task_id='execute_sp_material_retail_tier_price_vf',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_RETAIL_TIER_PRICE_VF();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_regulation_block = BigQueryInsertJobOperator(
        task_id='execute_sp_material_regulation_block',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_REGULATION_BLOCK();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_delta_load = BigQueryInsertJobOperator(
        task_id='execute_sp_material_delta_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_DELTA_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_legal_control = BigQueryInsertJobOperator(
        task_id='execute_sp_material_legal_control',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_LEGAL_CONTROL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_material_pricing_vf_delta_load = BigQueryInsertJobOperator(
        task_id='execute_sp_material_pricing_vf_delta_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_MATERIAL_PRICING_VF_DELTA_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    create_snow_alert = ServiceNowAlertingOperator(
        task_id='create_snow_alert',
        message=f"[PDW]pharma_pdwmtrl_material_dailyload_sqdc has failed or has failed tasks",
        severity='3',
        source='airflow.cardinalhealth.net',
        additional_details=f"Additional details of failure available at http://airflow.cardinalhealth.net/dags/pharma_pdwmtrl_material_dailyload_sqdc/grid",
        website=f"http://airflow.cardinalhealth.net",
        assignment_grp='Pharma-DnA',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Setting task dependencies
    execute_sp_rems_program >> execute_sp_hazmat_storage
    execute_sp_hazmat_storage >> execute_sp_cross_distrib_chain_mtrl_status
    execute_sp_cross_distrib_chain_mtrl_status >> execute_sp_hamacher_finer_class
    execute_sp_hamacher_finer_class >> execute_sp_dangerous_good_indicator
    execute_sp_dangerous_good_indicator >> execute_sp_hamacher_finest_class
    execute_sp_hamacher_finest_class >> execute_sp_pricing_usage
    execute_sp_pricing_usage >> execute_sp_cross_plant_material_status
    execute_sp_cross_plant_material_status >> execute_sp_list_1_chemical
    execute_sp_list_1_chemical >> execute_sp_special_handling
    execute_sp_special_handling >> execute_sp_material_group
    execute_sp_material_group >> execute_sp_item_category_group
    execute_sp_item_category_group >> execute_sp_temperature_condition
    execute_sp_temperature_condition >> execute_sp_industry_sector
    execute_sp_industry_sector >> execute_sp_ean_category
    execute_sp_ean_category >> execute_sp_hamacher_fine_class
    execute_sp_hamacher_fine_class >> execute_sp_package_reference
    execute_sp_package_reference >> execute_sp_form
    execute_sp_form >> execute_sp_packaging_indicator
    execute_sp_packaging_indicator >> execute_sp_storage_condition
    execute_sp_storage_condition >> execute_sp_product_allocation
    execute_sp_product_allocation >> execute_sp_transportation_group
    execute_sp_transportation_group >> execute_sp_packaging_material_type
    execute_sp_packaging_material_type >> execute_sp_material_type
    execute_sp_material_type >> execute_sp_product_hierarchy
    execute_sp_product_hierarchy >> execute_sp_legal_control
    execute_sp_legal_control >> execute_sp_dea_sub_base_code
    execute_sp_dea_sub_base_code >> execute_sp_dea_base_code
    execute_sp_dea_base_code >> execute_sp_sales_tax_driver
    execute_sp_sales_tax_driver >> execute_sp_ahfs_code
    execute_sp_ahfs_code >> execute_sp_orange_book_code
    execute_sp_orange_book_code >> execute_sp_unit_of_measure
    execute_sp_unit_of_measure >> execute_sp_mtrl_grp_packaging_materials
    execute_sp_mtrl_grp_packaging_materials >> execute_sp_material_class_rlt
    execute_sp_material_class_rlt >> execute_sp_material_display
    execute_sp_material_display >> execute_sp_material_sales_area_rlt
    execute_sp_material_sales_area_rlt >> execute_sp_material_equivalence
    execute_sp_material_equivalence >> execute_sp_material_plant_rlt
    execute_sp_material_plant_rlt >> execute_sp_material_retail_tier_price_vf
    execute_sp_material_retail_tier_price_vf >> execute_sp_material_regulation_block
    execute_sp_material_regulation_block >> execute_sp_material_delta_load
    execute_sp_material_delta_load >> execute_sp_material_legal_control
    execute_sp_material_legal_control >> execute_sp_material_pricing_vf_delta_load
    execute_sp_material_pricing_vf_delta_load >> create_snow_alert