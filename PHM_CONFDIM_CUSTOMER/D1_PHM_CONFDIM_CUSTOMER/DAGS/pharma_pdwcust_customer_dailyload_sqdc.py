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
    schedule_interval='045 5 * * 0'
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
    schedule_interval='045 5 * * 0'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    schedule_interval='045 5 * * *'


# DAG Definition
with DAG(
    'pharma_pdwcust_customer_dailyload_sqdc',
    default_args=default_args,
    description=f'Process for updating pharma_pdwcust_customer_dailyload_sqdc',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['pdw', 'customer','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:

    execute_sp_multiple_copies_preference = BigQueryInsertJobOperator(
        task_id='execute_sp_multiple_copies_preference',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_MULTIPLE_COPIES_PREFERENCE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_shelf_label_sticker_options = BigQueryInsertJobOperator(
        task_id='execute_sp_shelf_label_sticker_options',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SHELF_LABEL_STICKER_OPTIONS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_financial_reporting = BigQueryInsertJobOperator(
        task_id='execute_sp_financial_reporting',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_FINANCIAL_REPORTING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_printed_invoice_sequence = BigQueryInsertJobOperator(
        task_id='execute_sp_printed_invoice_sequence',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PRINTED_INVOICE_SEQUENCE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_legal_status = BigQueryInsertJobOperator(
        task_id='execute_sp_legal_status',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_LEGAL_STATUS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_suppress_printed_invoice = BigQueryInsertJobOperator(
        task_id='execute_sp_suppress_printed_invoice',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_PRINTED_INVOICE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fuel_surcharge = BigQueryInsertJobOperator(
        task_id='execute_sp_fuel_surcharge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_FUEL_SURCHARGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_ims_code = BigQueryInsertJobOperator(
        task_id='execute_sp_ims_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_IMS_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_tax_classification = BigQueryInsertJobOperator(
        task_id='execute_sp_tax_classification',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_TAX_CLASSIFICATION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_delivery_priority = BigQueryInsertJobOperator(
        task_id='execute_sp_delivery_priority',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_DELIVERY_PRIORITY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_spd_vitalsource_membership_type = BigQueryInsertJobOperator(
        task_id='execute_sp_spd_vitalsource_membership_type',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SPD_VITALSOURCE_MEMBERSHIP_TYPE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_stickers_price_masking = BigQueryInsertJobOperator(
        task_id='execute_sp_stickers_price_masking',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_STICKERS_PRICE_MASKING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_lot_expiration_preference = BigQueryInsertJobOperator(
        task_id='execute_sp_lot_expiration_preference',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_LOT_EXPIRATION_PREFERENCE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_store_size_special_grouping_all = BigQueryInsertJobOperator(
        task_id='execute_sp_store_size_special_grouping_all',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_STORE_SIZE_SPECIAL_GROUPING_ALL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_price_sticker = BigQueryInsertJobOperator(
        task_id='execute_sp_price_sticker',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PRICE_STICKER();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_storefront = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_storefront',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_STOREFRONT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_central_order_block = BigQueryInsertJobOperator(
        task_id='execute_sp_central_order_block',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CENTRAL_ORDER_BLOCK();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_parmed_customer_classification = BigQueryInsertJobOperator(
        task_id='execute_sp_parmed_customer_classification',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PARMED_CUSTOMER_CLASSIFICATION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_credit_control_area = BigQueryInsertJobOperator(
        task_id='execute_sp_credit_control_area',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CREDIT_CONTROL_AREA();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_central_delivery_block = BigQueryInsertJobOperator(
        task_id='execute_sp_central_delivery_block',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CENTRAL_DELIVERY_BLOCK();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_collection_group = BigQueryInsertJobOperator(
        task_id='execute_sp_collection_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_COLLECTION_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_returns_group = BigQueryInsertJobOperator(
        task_id='execute_sp_returns_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_RETURNS_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_edrp_sites = BigQueryInsertJobOperator(
        task_id='execute_sp_edrp_sites',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_EDRP_SITES();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_invoice_price_masking = BigQueryInsertJobOperator(
        task_id='execute_sp_invoice_price_masking',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_INVOICE_PRICE_MASKING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_c2_restriction = BigQueryInsertJobOperator(
        task_id='execute_sp_c2_restriction',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_C2_RESTRICTION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_retail_pricing_indicator = BigQueryInsertJobOperator(
        task_id='execute_sp_retail_pricing_indicator',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICING_INDICATOR();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_shelf_label_options = BigQueryInsertJobOperator(
        task_id='execute_sp_shelf_label_options',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SHELF_LABEL_OPTIONS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_stock_number_code = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_stock_number_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_STOCK_NUMBER_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_340b_contract_fee_policy = BigQueryInsertJobOperator(
        task_id='execute_sp_340b_contract_fee_policy',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_340B_CONTRACT_FEE_POLICY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_retail_price_tier = BigQueryInsertJobOperator(
        task_id='execute_sp_retail_price_tier',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_RETAIL_PRICE_TIER();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_incoterms = BigQueryInsertJobOperator(
        task_id='execute_sp_incoterms',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_INCOTERMS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_shipping_conditions = BigQueryInsertJobOperator(
        task_id='execute_sp_shipping_conditions',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SHIPPING_CONDITIONS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_shelf_label = BigQueryInsertJobOperator(
        task_id='execute_sp_shelf_label',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SHELF_LABEL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_central_billing_block = BigQueryInsertJobOperator(
        task_id='execute_sp_central_billing_block',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CENTRAL_BILLING_BLOCK();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_account_assignment_group = BigQueryInsertJobOperator(
        task_id='execute_sp_account_assignment_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_ACCOUNT_ASSIGNMENT_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_trading_partner_company = BigQueryInsertJobOperator(
        task_id='execute_sp_trading_partner_company',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_TRADING_PARTNER_COMPANY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_account_group = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_account_group',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_ACCOUNT_GROUP();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_primary_secondary_code = BigQueryInsertJobOperator(
        task_id='execute_sp_primary_secondary_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_SECONDARY_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_credit_memo_preferences = BigQueryInsertJobOperator(
        task_id='execute_sp_credit_memo_preferences',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CREDIT_MEMO_PREFERENCES();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_item_partial_delivery = BigQueryInsertJobOperator(
        task_id='execute_sp_item_partial_delivery',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_ITEM_PARTIAL_DELIVERY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_class_of_trade_level_one = BigQueryInsertJobOperator(
        task_id='execute_sp_class_of_trade_level_one',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CLASS_OF_TRADE_LEVEL_ONE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_suppress_zero_printing = BigQueryInsertJobOperator(
        task_id='execute_sp_suppress_zero_printing',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SUPPRESS_ZERO_PRINTING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_otc_restriction = BigQueryInsertJobOperator(
        task_id='execute_sp_otc_restriction',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_OTC_RESTRICTION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_industry_key = BigQueryInsertJobOperator(
        task_id='execute_sp_industry_key',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_INDUSTRY_KEY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_primary_business_unit = BigQueryInsertJobOperator(
        task_id='execute_sp_primary_business_unit',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BUSINESS_UNIT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_spd_physician_dispensing = BigQueryInsertJobOperator(
        task_id='execute_sp_spd_physician_dispensing',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_SPD_PHYSICIAN_DISPENSING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_backorder_restriction = BigQueryInsertJobOperator(
        task_id='execute_sp_backorder_restriction',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_BACKORDER_RESTRICTION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_class_of_trade_level_three = BigQueryInsertJobOperator(
        task_id='execute_sp_class_of_trade_level_three',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CLASS_OF_TRADE_LEVEL_THREE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_commitment_level = BigQueryInsertJobOperator(
        task_id='execute_sp_commitment_level',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_COMMITMENT_LEVEL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_pricing_procedure = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_pricing_procedure',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_PRICING_PROCEDURE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_primary_backorder_selection = BigQueryInsertJobOperator(
        task_id='execute_sp_primary_backorder_selection',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PRIMARY_BACKORDER_SELECTION();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_consignment_program = BigQueryInsertJobOperator(
        task_id='execute_sp_consignment_program',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CONSIGNMENT_PROGRAM();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_item_stickers_cost_masking = BigQueryInsertJobOperator(
        task_id='execute_sp_item_stickers_cost_masking',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_ITEM_STICKERS_COST_MASKING();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_payment_terms = BigQueryInsertJobOperator(
        task_id='execute_sp_payment_terms',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PAYMENT_TERMS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_partner_function_code = BigQueryInsertJobOperator(
        task_id='execute_sp_partner_function_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_PARTNER_FUNCTION_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_partner_function_rlt = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_partner_function_rlt',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_PARTNER_FUNCTION_RLT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_class_of_trade_level_two = BigQueryInsertJobOperator(
        task_id='execute_sp_class_of_trade_level_two',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CLASS_OF_TRADE_LEVEL_TWO();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_gpo_class_of_trade = BigQueryInsertJobOperator(
        task_id='execute_sp_gpo_class_of_trade',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_GPO_CLASS_OF_TRADE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_code = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_company_code = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_company_code',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_COMPANY_CODE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_business_activity = BigQueryInsertJobOperator(
        task_id='execute_sp_business_activity',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_BUSINESS_ACTIVITY();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_edi832_parameter = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_edi832_parameter',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_EDI832_PARAMETER();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_sales_area_rlt_delta_load = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_sales_area_rlt_delta_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_SALES_AREA_RLT_DELTA_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_delta_load = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_delta_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_DELTA_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    create_snow_alert = ServiceNowAlertingOperator(
        task_id='create_snow_alert',
        message=f"[PDW]pharma_pdwcust_customer_dailyload_sqdc has failed or has failed tasks",
        severity='3',
        source='airflow.cardinalhealth.net',
        additional_details=f"Additional details of failure available at http://airflow.cardinalhealth.net/dags/pharma_pdwcust_customer_dailyload_sqdc/grid",
        website=f"http://airflow.cardinalhealth.net",
        assignment_grp='Pharma-DnA',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Setting task dependencies
    execute_sp_multiple_copies_preference >> execute_sp_shelf_label_sticker_options
    execute_sp_shelf_label_sticker_options >> execute_sp_financial_reporting
    execute_sp_financial_reporting >> execute_sp_printed_invoice_sequence
    execute_sp_printed_invoice_sequence >> execute_sp_legal_status
    execute_sp_legal_status >> execute_sp_suppress_printed_invoice
    execute_sp_suppress_printed_invoice >> execute_sp_fuel_surcharge
    execute_sp_fuel_surcharge >> execute_sp_ims_code
    execute_sp_ims_code >> execute_sp_tax_classification
    execute_sp_tax_classification >> execute_sp_delivery_priority
    execute_sp_delivery_priority >> execute_sp_spd_vitalsource_membership_type
    execute_sp_spd_vitalsource_membership_type >> execute_sp_stickers_price_masking
    execute_sp_stickers_price_masking >> execute_sp_lot_expiration_preference
    execute_sp_lot_expiration_preference >> execute_sp_store_size_special_grouping_all
    execute_sp_store_size_special_grouping_all >> execute_sp_price_sticker
    execute_sp_price_sticker >> execute_sp_customer_storefront
    execute_sp_customer_storefront >> execute_sp_central_order_block
    execute_sp_central_order_block >> execute_sp_parmed_customer_classification
    execute_sp_parmed_customer_classification >> execute_sp_credit_control_area
    execute_sp_credit_control_area >> execute_sp_central_delivery_block
    execute_sp_central_delivery_block >> execute_sp_collection_group
    execute_sp_collection_group >> execute_sp_returns_group
    execute_sp_returns_group >> execute_sp_edrp_sites
    execute_sp_edrp_sites >> execute_sp_invoice_price_masking
    execute_sp_invoice_price_masking >> execute_sp_c2_restriction
    execute_sp_c2_restriction >> execute_sp_retail_pricing_indicator
    execute_sp_retail_pricing_indicator >> execute_sp_shelf_label_options
    execute_sp_shelf_label_options >> execute_sp_customer_stock_number_code
    execute_sp_customer_stock_number_code >> execute_sp_340b_contract_fee_policy
    execute_sp_340b_contract_fee_policy >> execute_sp_retail_price_tier
    execute_sp_retail_price_tier >> execute_sp_incoterms
    execute_sp_incoterms >> execute_sp_shipping_conditions
    execute_sp_shipping_conditions >> execute_sp_shelf_label
    execute_sp_shelf_label >> execute_sp_central_billing_block
    execute_sp_central_billing_block >> execute_sp_account_assignment_group
    execute_sp_account_assignment_group >> execute_sp_trading_partner_company
    execute_sp_trading_partner_company >> execute_sp_customer_account_group
    execute_sp_customer_account_group >> execute_sp_primary_secondary_code
    execute_sp_primary_secondary_code >> execute_sp_credit_memo_preferences
    execute_sp_credit_memo_preferences >> execute_sp_item_partial_delivery
    execute_sp_item_partial_delivery >> execute_sp_class_of_trade_level_one
    execute_sp_class_of_trade_level_one >> execute_sp_suppress_zero_printing
    execute_sp_suppress_zero_printing >> execute_sp_otc_restriction
    execute_sp_otc_restriction >> execute_sp_industry_key
    execute_sp_industry_key >> execute_sp_primary_business_unit
    execute_sp_primary_business_unit >> execute_sp_spd_physician_dispensing
    execute_sp_spd_physician_dispensing >> execute_sp_backorder_restriction
    execute_sp_backorder_restriction >> execute_sp_class_of_trade_level_three
    execute_sp_class_of_trade_level_three >> execute_sp_commitment_level
    execute_sp_commitment_level >> execute_sp_customer_pricing_procedure
    execute_sp_customer_pricing_procedure >> execute_sp_primary_backorder_selection
    execute_sp_primary_backorder_selection >> execute_sp_consignment_program
    execute_sp_consignment_program >> execute_sp_item_stickers_cost_masking
    execute_sp_item_stickers_cost_masking >> execute_sp_payment_terms
    execute_sp_payment_terms >> execute_sp_partner_function_code
    execute_sp_partner_function_code >> execute_sp_customer_partner_function_rlt
    execute_sp_customer_partner_function_rlt >> execute_sp_class_of_trade_level_two
    execute_sp_class_of_trade_level_two >> execute_sp_gpo_class_of_trade
    execute_sp_gpo_class_of_trade >> execute_sp_customer_code
    execute_sp_customer_code >> execute_sp_customer_company_code
    execute_sp_customer_company_code >> execute_sp_business_activity
    execute_sp_business_activity >> execute_sp_customer_edi832_parameter
    execute_sp_customer_edi832_parameter >> execute_sp_customer_sales_area_rlt_delta_load
    execute_sp_customer_sales_area_rlt_delta_load >> execute_sp_customer_delta_load
    execute_sp_customer_delta_load >> create_snow_alert