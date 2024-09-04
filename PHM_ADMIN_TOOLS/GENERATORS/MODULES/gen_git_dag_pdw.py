import pandas as pd
import os
from datetime import datetime
import numpy as np
import git_operations

# Define your repository path and branch name
repo_path = 'pharma-data-warehouse-edna/'
branch_name = 'admin_gen_dag_pdw'

# Call the function
git_operations.switch_to_branch(repo_path, branch_name)

project_id = '{project_id}'

def generate_dag_code(dag_metadata):
    # Initialize the DAG header
    header = f"""from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from custom_operators import ServiceNowAlertingOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {{
    'owner': 'pdna',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': ['{dag_metadata['dag_email']}'],
}}


# Fetch environment from Airflow Variables
airflow_env = Variable.get("env")
project_id = Variable.get("BQ_DATA_PROJECT")


# Initialize environment-specific variables
if airflow_env == "dev":
    service_account = '{dag_metadata['sa_dev']}'
    compute_project_id = '{dag_metadata['sa_dev'].split('@')[1].split('.')[0]}'
    schedule_interval={'None' if dag_metadata['sched_interval_dev'] is None else f"'{dag_metadata['sched_interval_dev']}'"}
elif airflow_env == "stg":
    service_account = '{dag_metadata['sa_stg']}'
    compute_project_id = '{dag_metadata['sa_stg'].split('@')[1].split('.')[0]}'
    schedule_interval={'None' if dag_metadata['sched_interval_stg'] is None else f"'{dag_metadata['sched_interval_stg']}'"}
elif airflow_env == "prd":
    service_account = '{dag_metadata['sa_prod']}'
    compute_project_id = '{dag_metadata['sa_prod'].split('@')[1].split('.')[0]}'
    schedule_interval={'None' if dag_metadata['sched_interval_prd'] is None else f"'{dag_metadata['sched_interval_prd']}'"}


# DAG Definition
with DAG(
    '{dag_metadata['dag_name']}',
    default_args=default_args,
    description=f'Process for updating {dag_metadata['dag_name']}',
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'),
    catchup=False,
    tags=['{dag_metadata['team']}', '{dag_metadata['dag_domain']}','{dag_metadata['dag_type'].lower()}'],  # Added the dag_domain as a tag
    access_control= {{"pdna" : {"can_read", "can_edit"}}},
    concurrency=50,  # Add this line to set the concurrency
    max_active_runs = 1,
) as dag:
"""


    # Generate task code
    tasks = ""
    for task in dag_metadata['tasks']:
        if task['task_type'] == 'CALL SP':
            query_str = f"CALL `{project_id}`.{task['table_schema']}.{task['sproc_call']};"
            tasks += f'''
    {task['task_id']} = BigQueryInsertJobOperator(
        task_id='{task['task_id']}',
        configuration={{
            'query': {{
                'query': f"{query_str}",
                'use_legacy_sql': False
            }}
        }},
        impersonation_chain=service_account,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=120), 
    )
'''
        elif task['task_type'] == 'TRIGGER DAG': #To Trigger another DAG
            tasks += f'''
    {task['task_id']} = TriggerDagRunOperator(
        task_id='{task['task_id']}',
        trigger_dag_id='{task['sproc_call']}', 
    )
'''

        elif task['task_type'] == 'CREATE ALERT': #To Generate SNOW alert for failures
            tasks += f'''
    {task['task_id']} = ServiceNowAlertingOperator(
        task_id='{task['task_id']}',
        message=f"[PDW]{dag_metadata['dag_name']} has failed or has failed tasks",
        severity='{dag_metadata['snow_severity']}',
        source='{dag_metadata['airflow_url']}',
        additional_details=f"Additional details of failure available at http://{dag_metadata['airflow_url']}/dags/{dag_metadata['dag_name']}/grid",
        website=f"http://{dag_metadata['airflow_url']}",
        assignment_grp='{dag_metadata['snow_assignment_group']}',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
'''

    # Generate Grouped Task List
    task_df = pd.DataFrame(columns=['seq','task_id'])
    index = 0
    for task in dag_metadata['tasks']: #Get the task seq & task id
        task_df.loc[index] = [task['task_no_seq'],task['task_id']]
        index = index + 1
    
    sorted_task_df=task_df.sort_values('seq')
    grouped_task_df = sorted_task_df.groupby('seq').transform(lambda x: ','.join(x)).drop_duplicates() #Group Parallel tasks in Single Row
    
    #Add attribute for next task list
    grouped_task_df['next_task_id'] = grouped_task_df.task_id.shift(-1)

    grouped_task_list=[]                         
    for index,row in grouped_task_df.iterrows():
        if (row['task_id']).__contains__(","):
            grouped_task_list.append({'task_id':"["+row['task_id']+"]"})
                #Add one Dummy task for consecutive parallel groups
            if (row['next_task_id']).__contains__(","):
                tasks += f'''
    execute_dummy_task_{index} = DummyOperator(
        task_id='execute_dummy_task_{index}', 
    )
'''
                grouped_task_list.append({'task_id':f"execute_dummy_task_{index}"})
        else:
            grouped_task_list.append({'task_id':row['task_id']})    
    
    # Generate task dependencies
    dependencies = "\n".join(f"    {task['task_id']} >> {next_task['task_id']}"
                            for task, next_task in zip(grouped_task_list[:-1], grouped_task_list[1:]))


    # Combine all parts to form the complete DAG code
    dag_code = header + tasks + "\n    # Setting task dependencies\n" + dependencies
    return dag_code


if __name__ == "__main__":
    # Read metadata CSV
    csv_path = 'pharma-data-warehouse-edna\PHM_ADMIN_TOOLS\INPUT_FILES\dag_metadata.csv'
    dag_metadata_df = pd.read_csv(csv_path)
    dag_metadata_df = dag_metadata_df[dag_metadata_df['Status'] == 'Ready']


    # Group metadata by DAG names
    grouped_dag_metadata = dag_metadata_df.groupby('dag_name')


    # Generate DAGs
    for dag_name, group in grouped_dag_metadata:
        sorted_group = group.sort_values('Task Number Seq')
        tasks = []
        for _, row in sorted_group.iterrows():
            task = {
                'task_id': f"execute_{row['table_name'].replace(' ', '_').lower()}",
                'task_no_seq': row['Task Number Seq'],
                'table_catalog': row['table_catalog'],
                'table_schema': row['table_schema'],
                'table_name': row['table_name'],
                'sproc_call': row['sproc_call'],
                'task_type': row['task_type'].upper()
            }
            tasks.append(task)


        sched_interval_prd = group['sched_interval'].iloc[0]
        if sched_interval_prd == 'None' or pd.isna(sched_interval_prd):
            sched_interval_prd = None
        
        sched_interval_dev = group['sched_interval_dev'].iloc[0]
        if sched_interval_dev == 'None' or pd.isna(sched_interval_dev):
            sched_interval_dev = None

        sched_interval_stg = group['sched_interval_stg'].iloc[0]
        if sched_interval_stg == 'None' or pd.isna(sched_interval_stg):
            sched_interval_stg = None


        table_schema_group = group['table_schema_group'].iloc[0]  # Added this line
        table_schema = group['table_schema'].iloc[0]
        sa_dev = group['sa_dev'].iloc[0]  # Added this line
        sa_stg = group['sa_stg'].iloc[0]  # Added this line
        sa_prod = group['sa_prod'].iloc[0]  # Added this line
        team = group['team'].iloc[0]  # Added this line

        snow_alert = group['snow_alert'].iloc[0]
        if snow_alert == 'Y':
            snow_task = {
                'task_id': 'create_snow_alert',
                'task_no_seq': 999999,
                'table_catalog': '',
                'table_schema': '',
                'table_name': '',
                'sproc_call': '',
                'task_type': 'CREATE ALERT'
            }
            tasks.append(snow_task)

        output_dir = f'pharma-data-warehouse-edna/{table_schema_group}/{table_schema}/DAGS'


        os.makedirs(output_dir, exist_ok=True)


        dag_metadata = {
            'dag_name': dag_name,
            'tasks': tasks,
            'sched_interval_prd': sched_interval_prd,
            'sched_interval_dev': sched_interval_dev,
            'sched_interval_stg': sched_interval_stg,
            'sa_dev': sa_dev,
            'sa_stg': sa_stg,
            'sa_prod': sa_prod,
            'team': team,
            'dag_email': group['dag_email'].iloc[0],
            'dag_domain': group['dag_domain'].iloc[0],  # Added this line
            'dag_type': group['dag_type'].iloc[0], # Added this line
            'snow_assignment_group' : group['snow_assignment_group'].iloc[0],
            'snow_severity' : group['snow_severity'].iloc[0],
            'airflow_url' : group['airflow_url'].iloc[0]
        }


        generated_dag_code = generate_dag_code(dag_metadata)


        # Save the generated DAGs to files
        file_path = os.path.join(output_dir, f"{dag_name}.py")
        with open(file_path, 'w') as f:
            f.write(generated_dag_code)