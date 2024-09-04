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
    schedule_interval='030 07 * * 0'
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
    schedule_interval='030 07 * * 0'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    schedule_interval='030 07 * * *'


# DAG Definition
with DAG(
    'pharma_pdwfdb_fdb_dailyload_sqdc',
    default_args=default_args,
    description=f'Process for updating pharma_pdwfdb_fdb_dailyload_sqdc',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['pdw', 'fdb','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:

    execute_sp_fdb_audit = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_audit',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_AUDIT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_drug_disease = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_drug_disease',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_DISEASE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_drug_imprints = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_drug_imprints',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_DRUG_IMPRINTS();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_hierarchical_ingredient = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_hierarchical_ingredient',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_HIERARCHICAL_INGREDIENT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_master = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_master',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_MASTER();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_ndc_price = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_ndc_price',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_NDC_PRICE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_fdb_ndc_attribute = BigQueryInsertJobOperator(
        task_id='execute_sp_fdb_ndc_attribute',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_MATERIAL.SP_FDB_NDC_ATTRIBUTE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    create_snow_alert = ServiceNowAlertingOperator(
        task_id='create_snow_alert',
        message=f"[PDW]pharma_pdwfdb_fdb_dailyload_sqdc has failed or has failed tasks",
        severity='3',
        source='airflow.cardinalhealth.net',
        additional_details=f"Additional details of failure available at http://airflow.cardinalhealth.net/dags/pharma_pdwfdb_fdb_dailyload_sqdc/grid",
        website=f"http://airflow.cardinalhealth.net",
        assignment_grp='Pharma-DnA',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Setting task dependencies
    execute_sp_fdb_audit >> execute_sp_fdb_drug_disease
    execute_sp_fdb_drug_disease >> execute_sp_fdb_drug_imprints
    execute_sp_fdb_drug_imprints >> execute_sp_fdb_hierarchical_ingredient
    execute_sp_fdb_hierarchical_ingredient >> execute_sp_fdb_master
    execute_sp_fdb_master >> execute_sp_fdb_ndc_price
    execute_sp_fdb_ndc_price >> execute_sp_fdb_ndc_attribute
    execute_sp_fdb_ndc_attribute >> create_snow_alert