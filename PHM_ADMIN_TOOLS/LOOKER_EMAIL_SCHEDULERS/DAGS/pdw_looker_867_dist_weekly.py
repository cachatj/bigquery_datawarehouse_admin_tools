from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.kubernetes.secret import Secret
import pendulum
from custom_operators import ServiceNowAlertingOperator
from airflow.utils.trigger_rule import TriggerRule

# Fetch environment from Airflow Variables
airflow_env = Variable.get("env")
smtp_password = Variable.get("PDW_LOOKER_SMTP")

args = {
    'owner': 'pdna',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email': ['G-EIT-PharmaDnA-Run@cardinalhealth.com'],
}

email_to = ["G-EIT-PharmaDnA-Run@cardinalhealth.com"]
looker_client_id = "edna-phm-dw-looker-client-id"
looker_client_secret = "edna-phm-dw-looker-client-key"

if airflow_env == "dev":
    namespace = "edna-data-dev"
    service_account_name = (
        "sa-edna-pharma-data-whse@edc-phmdw-np-cah.iam.gserviceaccount.com"
    )
    lookerBaseURL = "https://lookerinternaldev.cardinalhealth.net:19999"
    server_smtp = "devinternalsmtpauth.cardinalhealth.net:25"
    eddieProjectName = "edc-phmdw-np-cah"
    distListTableName = "PROJECT_ID.VW_PHM_CONFFACT_BILLING.EDI_867_DIST_LIST_CV"
    smtp_user = ""
    cc_gmb_address = "GMB-DUB-DCOE@cardinalhealth.com"
    schedule_interval = None
elif airflow_env == "stg":
    namespace = "eddie-stg-pr"
    service_account_name = (
        "sa-edna-pharma-data-whse@edc-phmdwstg-pr-cah.iam.gserviceaccount.com"
    )
    lookerBaseURL = "https://lookerinternalstg.cardinalhealth.net:19999"
    server_smtp = "MpgSMTPAuth.cardinalhealth.net:25"
    eddieProjectName = "edc-phmdwstg-pr-cah"
    distListTableName = "PROJECT_ID.VW_PHM_CONFFACT_BILLING.EDI_867_DIST_LIST_CV"
    smtp_user = "ap-30421.lookerk8ssc"
    cc_gmb_address = "GMB-DUB-DCOE@cardinalhealth.com"
    schedule_interval = None
elif airflow_env == "prd":
    namespace = "eddie-pr"
    service_account_name = (
        "sa-edna-pharma-data-whse@edc-phmdw-pr-cah.iam.gserviceaccount.com"
    )
    lookerBaseURL = "https://lpec5009look01.cardinalhealth.net:19999"
    server_smtp = "dubSMTPAuth.cardinalhealth.net:25"
    eddieProjectName = "edc-phmdw-pr-cah"
    distListTableName = "PROJECT_ID.VW_PHM_CONFFACT_BILLING.EDI_867_DIST_LIST_CV"
    smtp_user = "ap-30421.lookerk8ssc"
    cc_gmb_address = "GMB-DUB-DCOE@cardinalhealth.com"
    schedule_interval = "15 7  * * 1"

secret_client_id = Secret(
    deploy_type='volume',
    deploy_target='/var/secrets/key/id',
    secret=looker_client_id
)
 
secret_client_secret = Secret(
    deploy_type='volume',
    deploy_target='/var/secrets/key/pkey',
    secret=looker_client_secret
)

with DAG(
    dag_id="pdw_looker_867_dist_weekly",
    default_args=args,
    schedule_interval=schedule_interval,
    start_date=pendulum.datetime(2023, 9, 8, tz='US/Eastern'), #days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["pdw", "tdr", "looker", "api", "docker", "gke"],
    access_control={
        "pdna": ("can_read", "can_edit")
    },  # Added Missing Access Control (JH)
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    deploy_gke_pod = KubernetesPodOperator(
        # k8s namespace created earlier
        namespace=namespace,
        # k8s service account name created earlier
        # service_account_name="sa-edna-pharma-data-whse@edc-phmdw-np-cah.iam.gserviceaccount.com",
        # Ensures that cache is always refreshed
        image_pull_policy="Always",
        # Artifact image of dbt repo
        image="harbor.dev1.cardinalhealth.net/eddie/pdw-lookerapi-reports:0.3",
        secrets=[secret_client_id, secret_client_secret],
        # links to ENTRYPOINT in .sh file
        cmds=["python", "-u", "main.py"],
        env_vars={
            "SERVICE_ACCOUNT": service_account_name,
            "LOOKERSDK_BASE_URL": lookerBaseURL,
            "LOOKER_PHARMA_CLIENT": "/var/secrets/key/id/client_id",
            "LOOKER_PHARMA_SECRET": "/var/secrets/key/pkey/client_secret",
            "SMTP_HOST": server_smtp,
            "SMTP_USER_NAME": smtp_user,
            "SMTP_PASSWORD": smtp_password
        },
        # Arguments to the entrypoint. The docker image's CMD is used if this
        # is not provided. The arguments parameter is templated.
        arguments=[
            "send-to-email",
            "pharma_data_warehouse::867",
            distListTableName,
            eddieProjectName,
            "Weekly",
            "867",
            cc_gmb_address,
        ],
        name="cardinal_send_to_email",
        task_id="send_to_email",
        get_logs=True,
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)

    create_snow_alert = ServiceNowAlertingOperator(
        task_id='create_snow_alert',
        message=f"[PDW]pdw_looker_867_dist_weekly has failed or has failed tasks",
        severity='3',
        source='airflow.cardinalhealth.net',
        additional_details=f"Additional details of failure available at http://airflow.cardinalhealth.net/dags/pdw_looker_867_dist_weekly/grid",
        website=f"http://airflow.cardinalhealth.net",
        assignment_grp='Pharma-DnA',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (start >> deploy_gke_pod >> end >> create_snow_alert)
