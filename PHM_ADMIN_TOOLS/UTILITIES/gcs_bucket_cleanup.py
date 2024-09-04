from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import os
import glob

# Default arguments for the DAG
default_args = {
    'owner': 'pdna',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': ['animita.das01@cardinalhealth.com'],
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
 
#Delete Files from GCS 
def delete_files_from_bucket(service_account,bucket_nm,file_pattern):
    hook = GCSHook(impersonation_chain=service_account)
    print(bucket_nm)
    print(file_pattern)
    #Get File list from GCS Bucket
    objects = hook.list(bucket_name=bucket_nm,match_glob=file_pattern)
    for obj in objects:
        #Delete file from GCS Bucket
        hook.delete(bucket_name=bucket_nm,object_name=obj)

#Assign Variables
bucket_nm = '{{ dag_run.conf["bucket_name"] }}'
file_pattern = '{{ dag_run.conf["file_pattern"] }}'

with DAG("gcs_bucket_cleanup",
         default_args=default_args,
         description=f'This is used for Cleaning up files from GCS Bucket in Dev & Stage',
         schedule_interval=None,
         start_date=datetime(2023, 9, 8),
         catchup=False,
         tags=['pdw', 'gcs', 'cleanup'],  # Added the dag_domain as a tag
         access_control= {"pdna" : ('can_read', 'can_edit')},
         params = {'bucket_name': 'cah-bucket-name', 'file_pattern': '*.*'},
         ) as dag:
    
    execute_bucket_cleanup = PythonOperator(
        task_id = "execute_bucket_cleanup",
        python_callable =lambda: delete_files_from_bucket(service_account,f"'{bucket_nm}'",f"'{file_pattern}'"),
        dag=dag,
	)

     
    # Setting task dependencies