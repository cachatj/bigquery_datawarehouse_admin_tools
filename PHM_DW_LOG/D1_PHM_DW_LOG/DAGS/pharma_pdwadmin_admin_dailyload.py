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
    'email': ['joseph.holmesmeyer@cardinalhealth.com'],
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
    'pharma_pdwadmin_admin_dailyload',
    default_args=default_args,
    description=f'Process for updating pharma_pdwadmin_admin_dailyload',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['pdw', 'admin','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:

    execute_sp_admin_tables_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_tables_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_TABLES_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_columns_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_columns_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_COLUMNS_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_columns_desc_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_columns_desc_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_COLUMNS_DESC_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_views_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_views_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_VIEWS_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_table_storage_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_table_storage_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_TABLE_STORAGE_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_table_storage_history_insert = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_table_storage_history_insert',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_TABLE_STORAGE_HISTORY_INSERT();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_routines_merge = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_routines_merge',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_ROUTINES_MERGE();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_tables_columns_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_tables_columns_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_TABLES_COLUMNS_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_jobs_pdw_task_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_jobs_pdw_task_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_TASK_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_jobs_pdw_dag_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_jobs_pdw_dag_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_DAG_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_jobs_pdw_airflow_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_jobs_pdw_airflow_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_JOBS_PDW_AIRFLOW_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_entity_summary_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_entity_summary_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_ENTITY_SUMMARY_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_entity_inventory_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_entity_inventory_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_ENTITY_INVENTORY_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    execute_sp_admin_lineage_truncate_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_lineage_truncate_load',
        configuration={
            'query': {
                'query': f"CALL `{project_id}`.D1_PHM_DW_LOG.SP_ADMIN_LINEAGE_TRUNCATE_LOAD();",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )

    # Setting task dependencies
    execute_sp_admin_tables_merge >> execute_sp_admin_columns_merge
    execute_sp_admin_columns_merge >> execute_sp_admin_columns_desc_merge
    execute_sp_admin_columns_desc_merge >> execute_sp_admin_views_merge
    execute_sp_admin_views_merge >> execute_sp_admin_table_storage_merge
    execute_sp_admin_table_storage_merge >> execute_sp_admin_table_storage_history_insert
    execute_sp_admin_table_storage_history_insert >> execute_sp_admin_routines_merge
    execute_sp_admin_routines_merge >> execute_sp_admin_tables_columns_truncate_load
    execute_sp_admin_tables_columns_truncate_load >> execute_sp_admin_jobs_pdw_task_truncate_load
    execute_sp_admin_jobs_pdw_task_truncate_load >> execute_sp_admin_jobs_pdw_dag_truncate_load
    execute_sp_admin_jobs_pdw_dag_truncate_load >> execute_sp_admin_jobs_pdw_airflow_truncate_load
    execute_sp_admin_jobs_pdw_airflow_truncate_load >> execute_sp_admin_entity_summary_truncate_load
    execute_sp_admin_entity_summary_truncate_load >> execute_sp_admin_entity_inventory_truncate_load
    execute_sp_admin_entity_inventory_truncate_load >> execute_sp_admin_lineage_truncate_load