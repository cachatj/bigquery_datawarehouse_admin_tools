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
    schedule_interval=None
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
    schedule_interval=None
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    schedule_interval=None


# DAG Definition
with DAG(
    'pharma_pdwcust_customer_sp_customer_sales_area_rlt_initial_load',
    default_args=default_args,
    description=f'Process for updating pharma_pdwcust_customer_sp_customer_sales_area_rlt_initial_load',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['pdw', 'customer','init_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:

    execute_sp_customer_sales_area_rlt_initial_load = BigQueryInsertJobOperator(
        task_id='execute_sp_customer_sales_area_rlt_initial_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_CONFDIM_CUSTOMER.SP_CUSTOMER_SALES_AREA_RLT_INITIAL_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    # Setting task dependencies
