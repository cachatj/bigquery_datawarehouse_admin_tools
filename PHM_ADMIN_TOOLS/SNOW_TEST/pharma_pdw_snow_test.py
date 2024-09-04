from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago, timedelta
from datetime import datetime
from pytz import timezone
from airflow.models import Variable
from custom_operators import ServiceNowAlertingOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

"""Source identical BQ to GCS with temp table. This is necessary because the BQ job operator does not support directly going from BQ views to GCS."""
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
elif airflow_env == "stg":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdwstg-pr-cah'
elif airflow_env == "prd":
    service_account = 'sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com'
    compute_project_id = 'edc-phmdw-pr-cah'
    airflow_env = 'prod'

def raise_exception_test():
   raise RuntimeError('This exception is for testing airflow alerting.')
   
dag_name = "pharma_pdw_snow_test"

with DAG("pharma_pdw_snow_test",
         default_args=default_args,
         description=f'This is used for testing SNOW Alert',
         schedule_interval=None,
         start_date=datetime(2023, 9, 8),
         catchup=False,
         tags=['pdw', 'snow_test'],  # Added the dag_domain as a tag
         access_control= {"pdna" : ('can_read', 'can_edit')},
         ) as dag:
    
    #Raise error
    execute_raise_error = PythonOperator(
       task_id='execute_raise_error',
       python_callable=raise_exception_test,
       dag=dag
    )
        
    execute_dummy_task = EmptyOperator(
        task_id='execute_dummy_task',
        dag=dag
    )
    
    """Create SNOW Incident"""
    execute_snow_alert = ServiceNowAlertingOperator(
       task_id='execute_snow_alert',
       message=f"[PDW]{dag_name} has failed or has failed tasks",
       severity="3",
       source='airflow.cardinalhealth.net',
       additional_details=f"Additional details of failure available at http://airflow.cardinalhealth.net/dags/{dag_name}/grid",
       website=f"http://airflow.cardinalhealth.net",
       assignment_grp='EIT-TeradataRetirement-TDR',
       trigger_rule=TriggerRule.ONE_FAILED
   )

    # Setting task dependencies
    execute_raise_error >> execute_dummy_task
    execute_dummy_task >> execute_snow_alert
