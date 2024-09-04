from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule


# Default arguments for the DAG
default_args = {
    'owner': 'pdna',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': ['G-GTBS-Pharma-CommEn@cardinalhealth.com'],
}


# Fetch environment from Airflow Variables
airflow_env = Variable.get("env")
project_id = Variable.get("BQ_DATA_PROJECT")


# Initialize environment-specific variables
if airflow_env == "dev":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-np-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-np-cah'
    schedule_interval='00 17 * * 0'
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
    schedule_interval='00 17 * * 0'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    schedule_interval='00 17 * * *'


# DAG Definition
with DAG(
    'pharma_pdwpricing_pricing_dailyload_sqdc',
    default_args=default_args,
    description=f'Process for updating pharma_pdwpricing_pricing_dailyload_sqdc',
    schedule_interval=schedule_interval,
    start_date=datetime(2023, 9, 8),
    catchup=False,
    tags=['pdw', 'pricing','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
) as dag:

    execute_sp_customer_buy_grp_mtrl_block_vf = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_buy_grp_mtrl_block_vf',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_BUY_GRP_MTRL_BLOCK_VF();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_buy_grp_supplier_block_vf = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_buy_grp_supplier_block_vf',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_BUY_GRP_SUPPLIER_BLOCK_VF();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_cntrct_mtrl_block_vf = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_cntrct_mtrl_block_vf',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_MTRL_BLOCK_VF();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_customer_cntrct_supplier_block_vf = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_cntrct_supplier_block_vf',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFFACT_PRICING.SP_CUSTOMER_CNTRCT_SUPPLIER_BLOCK_VF();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_local_incl_excl_custlist_mtrl = BigQueryInsertJobOperator(
        task_id='execute_sp_local_incl_excl_custlist_mtrl',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_LOCAL_INCL_EXCL_CUSTLIST_MTRL();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    # Setting task dependencies
    execute_sp_customer_buy_grp_mtrl_block_vf >> execute_sp_customer_buy_grp_supplier_block_vf
    execute_sp_customer_buy_grp_supplier_block_vf >> execute_sp_customer_cntrct_mtrl_block_vf
    execute_sp_customer_cntrct_mtrl_block_vf >> execute_sp_customer_cntrct_supplier_block_vf
    execute_sp_customer_cntrct_supplier_block_vf >> execute_sp_local_incl_excl_custlist_mtrl