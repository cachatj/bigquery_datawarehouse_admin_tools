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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['joseph.holmesmeyer@cardinalhealth.com'],
}


# Fetch environment from Airflow Variables
airflow_env = Variable.get("env")
project_id = Variable.get("BQ_DATA_PROJECT")


# Initialize environment-specific variables
if airflow_env == "dev":
    service_account = 'sa-edna-pharma-data-whse@ednacomp-phm-np-cah.iam.gserviceaccount.com'
    compute_project_id = 'ednacomp-phm-np-cah'
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@ednacomp-phmstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'ednacomp-phmstg-pr-cah'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@ednacomp-phm-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'ednacomp-phm-pr-cah'


# DAG Definition
with DAG(
    'pharma_pdwadmin_dailyload',
    default_args=default_args,
    description=f'Process for updating pharma_pdwadmin_dailyload',
    schedule_interval='02 13-20/1 * * 1-5',
    start_date=datetime(2023, 9, 8),
    catchup=False,
    tags=['pdw', 'tdr', 'admin','seq_load'],  # Added the dag_domain as a tag
    access_control= {"pdna" : ('can_read', 'can_edit')},
) as dag:

    execute_sp_admin_jobs_delta_load = BigQueryInsertJobOperator(
        task_id='execute_sp_admin_jobs_delta_load',
        configuration={
            'query': {
                'query': f""" DECLARE
                            last_load_timestamp timestamp;
                            SET
                            last_load_timestamp = (
                            SELECT
                                IFNULL(MAX(creation_time),'2023-09-01 00:00:00')
                            FROM
                                `{project_id}`.D1_PHM_DW_LOG.ADMIN_JOBS_T);
                            INSERT INTO
                            `{project_id}`.D1_PHM_DW_LOG.ADMIN_JOBS_T
                            SELECT
                            CURRENT_DATE() AS snapshot_date,
                            CURRENT_TIMESTAMP() AS snapshot_timestamp,
                            *,
                            FROM
                            `{compute_project_id}.region-us`.INFORMATION_SCHEMA.JOBS
                            WHERE creation_time > last_load_timestamp;""",
                'use_legacy_sql': False
            }
        },
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
    )

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
    )

    # Setting task dependencies
    
    execute_sp_admin_jobs_delta_load >> execute_sp_admin_tables_merge
    execute_sp_admin_tables_merge >> execute_sp_admin_columns_merge
    execute_sp_admin_columns_merge >> execute_sp_admin_columns_desc_merge
    execute_sp_admin_columns_desc_merge >> execute_sp_admin_views_merge
    execute_sp_admin_views_merge >> execute_sp_admin_table_storage_merge
    execute_sp_admin_table_storage_merge >> execute_sp_admin_routines_merge
    execute_sp_admin_routines_merge >> execute_sp_admin_tables_columns_truncate_load
    execute_sp_admin_tables_columns_truncate_load >> execute_sp_admin_jobs_pdw_task_truncate_load
    execute_sp_admin_jobs_pdw_task_truncate_load >> execute_sp_admin_jobs_pdw_dag_truncate_load
    execute_sp_admin_jobs_pdw_dag_truncate_load >> execute_sp_admin_jobs_pdw_airflow_truncate_load
    execute_sp_admin_jobs_pdw_airflow_truncate_load >> execute_sp_admin_entity_summary_truncate_load
    execute_sp_admin_entity_summary_truncate_load >> execute_sp_admin_entity_inventory_truncate_load
    execute_sp_admin_entity_inventory_truncate_load >> execute_sp_admin_lineage_truncate_load