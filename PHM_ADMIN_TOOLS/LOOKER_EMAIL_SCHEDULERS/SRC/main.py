import traceback
import os

import dotenv
import typer
from typing_extensions import Annotated

from sdk_api40_connection.sdk_connection import SDKConnection
from cloudstorage.cloudstorage import CloudStorage
from bigquery.bigquery import BigQuery
from emailclient.smtpclient import SmtpClient
from dashboard_downloader.dashboard_downloader import (
    DashboardDownloader,
    create_zip_file,
)
from datetime import datetime, timedelta

app = typer.Typer()

dotenv.load_dotenv()

def last_friday() -> datetime:
    """Generates the last Friday from todays date

    Returns:
        datetime: datetime string
    """
    today = datetime.today()
    # Find out how many days have passed since the last Friday
    days_since_friday = (today.weekday() - 4) % 7
    if days_since_friday == 0:
        days_since_friday = 7
    # Subtract those days from today
    last_friday_date = today - timedelta(days=days_since_friday)
    return last_friday_date


def last_day_previous_month() -> datetime:
    """Generates the last day of the previous month

    Returns:
        datetime: a datetime object representing the date
    """
    today = datetime.today()
    # Move to the first day of the current month
    first_day_of_current_month = today.replace(day=1)
    # Subtract one day to get the last day of the previous month
    last_day_of_previous_month_date = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month_date

@app.command()
def create_in_bucket(
    dashboardid: Annotated[str, typer.Argument(help="The Dashboard ID to Email")],
    distlisttablename: Annotated[
        str,
        typer.Argument(
            help="The Location of the BQ dataset holding the distribution infomration"
        ),
    ],
    bucket_name: Annotated[str, typer.Argument(help="Google Bucket name")],
    key_file: Annotated[str, typer.Argument(help="Local path to key json file")],
    frequency: Annotated[
        str, typer.Argument(help="Frequency to Query the Distribution Table")
    ],
    schedule_type: Annotated[str, typer.Argument(help="852 or 867")],
):
    """
    Use for debugging write files to local-path instead of email
    """
    sdk_conn = SDKConnection(os.getenv("LOOKERSDK_BASE_URL"))
    sdk = sdk_conn.get_sdk_connection()

    try:
        #storage_bucket = CloudStorage(bucket_name, key_file)
        bigquery_client = BigQuery(key_file)

        # QUERY = f""" SELECT ID, USER_ATTRIBUTE, EMAIL, SUBJECT
        # FROM `{distlisttablename}`"""

        if schedule_type == "852":
            QUERY = f"""SELECT REPLACE(UPPER(FILENAME),' ','_') AS FileName,
            SUPPLIERNUMBER AS SupplierNumber, 
            UPPER(FREQUENCY) AS Frequency, 
            EMAILADDRESS AS EmailAddress, 
            MATERIALSTATUS as MaterialStatus FROM `{distlisttablename}`
            WHERE UPPER(FREQUENCY) = '{frequency.upper()}'
            """

        elif schedule_type == "867":
            # QUERY = f"""SELECT 'AMNEAL_PHARMACEUTICALS_CARDINAL_HEALTH_WEEKLY_867' AS FileName,
            # '0000004511' AS SupplierNumber, 
            # 'MONTHLY' AS Frequency, 
            # 'rajesh.pudota@cardinalhealth.com' AS EmailAddress
            # FROM `{distlisttablename}`
            # """

            QUERY = f"""SELECT REPLACE(UPPER(FILENAME),' ','_') AS FileName,
            SUPPLIERNUMBER AS SupplierNumber, 
            UPPER(FREQUENCY) AS Frequency, 
            EMAILADDRESS AS EmailAddress
            FROM `{distlisttablename}`
            WHERE UPPER(FREQUENCY) = '{frequency.upper()}'
            """

        records = bigquery_client.read_data(QUERY)

        if schedule_type == "852" and frequency == "Weekly":
            date_filter = last_friday()
        elif schedule_type == "852" and frequency == "Monthly":
            date_filter = last_day_previous_month()
        elif schedule_type == "867" and frequency == "Weekly":
            date_filter = "last week"
        elif schedule_type == "867" and frequency == "Monthly":
            date_filter = "last month"
        else:
            raise Exception()

        for record in records:

            if schedule_type == "852" and record.MaterialStatus == "Active":
                dashboard_filters = {
                    "Snapshot Date": date_filter.strftime("%Y/%m/%d"),
                    "Supplier Number": record.SupplierNumber,
                    "Current Version Flag": "Y",
                    "Material Status": "Active",
                }
            elif schedule_type == "852" and record.MaterialStatus == "All":
                dashboard_filters = {
                    "Snapshot Date": date_filter.strftime("%Y/%m/%d"),
                    "Supplier Number": record.SupplierNumber,
                    "Current Version Flag": "Y",
                }
            elif schedule_type == "867":
                dashboard_filters = {
                    "Billing Date": date_filter,
                    "Supplier Number": record.SupplierNumber,
                }

            try:
                if schedule_type == "852":
                    limit = 99999
                else:
                    limit = -1
                downloader = DashboardDownloader(
                    sdk=sdk,
                    dashboard_id=dashboardid,
                    dashboard_filters=dashboard_filters,
                    limit=limit,
                )
                downloader.create_tasks()
                files = downloader.get_tasks_results()

                compressed_xlsx = create_zip_file(files)

                with open(
                    f"./sample_results/{record.SupplierNumber}_{record.FileName}.zip", "wb"
                ) as f:  # use `wb` mode
                    f.write(compressed_xlsx.getvalue())
                # storage_bucket.upload_file(
                #     f"{schedule_type}/{record.SupplierNumber}_{record.FileName}.zip",
                #     compressed_xlsx,
                # )
                print(f"Report: {record.FileName} was uploaded to {bucket_name}")
            # pylint: disable=broad-exception-caught
            except Exception as e:
                print(f"Report: {record.FileName} was not sent to uploaded")
                print(e)
    except:
        print("Could not download dashboard")
        print(traceback.format_exc())
        raise


@app.command()
def send_to_email(
    dashboardid: Annotated[str, typer.Argument(help="The Dashboard ID to Email")],
    distlisttablename: Annotated[
        str,
        typer.Argument(
            help="The Location of the BQ dataset holding the distribution infomration"
        ),
    ],
    key_file: Annotated[str, typer.Argument(help="Local path to key json file")],
    frequency: Annotated[
        str, typer.Argument(help="Frequency to Query the Distribution Table")
    ],
    schedule_type: Annotated[str, typer.Argument(help="852 or 867")],
    to_cc: Annotated[str, typer.Argument(help="GMB-DUB-DCOE@cardinalhealth.com")],
    to_email: Annotated[
        str, typer.Option(help="For dev, email to send reports to")
    ] = None,
):
    """
    Use to email Looker Dashboards as zip files
    """

    sdk_conn = SDKConnection(os.environ.get("LOOKERSDK_BASE_URL"))
    sdk = sdk_conn.get_sdk_connection()

    try:
        email_client = SmtpClient()
        bigquery_client = BigQuery(key_file)

        if schedule_type == "852":
            QUERY = f"""SELECT REPLACE(UPPER(FILENAME),' ','_') AS FileName,
            SUPPLIERNUMBER AS SupplierNumber, 
            UPPER(FREQUENCY) AS Frequency, 
            EMAILADDRESS AS EmailAddress, 
            MATERIALSTATUS as MaterialStatus FROM `{distlisttablename}`
            WHERE UPPER(FREQUENCY) = '{frequency.upper()}'
            """

        elif schedule_type == "867":
            QUERY = f"""SELECT REPLACE(UPPER(FILENAME),' ','_') AS FileName,
            SUPPLIERNUMBER AS SupplierNumber, 
            UPPER(FREQUENCY) AS Frequency, 
            EMAILADDRESS AS EmailAddress
            FROM `{distlisttablename}`
            WHERE UPPER(FREQUENCY) = '{frequency.upper()}'
            """

        records = bigquery_client.read_data(QUERY)
        print(records)

        if schedule_type == "852" and frequency == "Weekly":
            date_filter = last_friday()
        elif schedule_type == "852" and frequency == "Monthly":
            date_filter = last_day_previous_month()
        elif schedule_type == "867" and frequency == "Weekly":
            date_filter = "last week"
        elif schedule_type == "867" and frequency == "Monthly":
            date_filter = "last month"
        else:
            raise Exception("Could not determine date")

        records = bigquery_client.read_data(QUERY)

        for record in records:
            if schedule_type == "852" and record.MaterialStatus == "Active":
                dashboard_filters = {
                    "Snapshot Date": date_filter.strftime("%Y/%m/%d"),
                    "Supplier Number": record.SupplierNumber,
                    "Current Version Flag": "Y",
                    "Material Status": "Active",
                }
            elif schedule_type == "852" and record.MaterialStatus == "All":
                dashboard_filters = {
                    "Snapshot Date": date_filter.strftime("%Y/%m/%d"),
                    "Supplier Number": record.SupplierNumber,
                    "Current Version Flag": "Y",
                }
            elif schedule_type == "867":
                dashboard_filters = {
                    "Billing Date": date_filter,
                    "Supplier Number": record.SupplierNumber,
                }

            try:
                if schedule_type == "852":
                    limit = 99999
                else:
                    limit = -1

                downloader = DashboardDownloader(
                    sdk=sdk,
                    dashboard_id=dashboardid,
                    dashboard_filters=dashboard_filters,
                    limit=limit,
                )

                downloader.create_tasks()
                files = downloader.get_tasks_results()

                if to_email:
                    destination = to_email
                else:
                    destination =  record.EmailAddress

                compressed_xlsx = create_zip_file(files)
                email_client.send_message_with_zip_report(
                    destination=destination,
                    mail_cc=to_cc,
                    subject=f"{record.FileName}",
                    content="""Please find the attached report from Cardinal Health.

Thank you,
Manufacturer Analytics Services
GMB-DUB-DCOE@cardinalhealth.com""",
                    filename=f"{record.FileName}.zip",
                    attachment=compressed_xlsx,
                )
                print(f"Report: {record.FileName} was sent to {destination}")
            # pylint: disable=broad-exception-caught
            except Exception as e:
                print(f"Report: {record.FileName} was not sent to {destination}")
                print(e)
    except:
        print("Could not email dashboards")
        # print(traceback.format_exc())
        raise


@app.command()
def create_in_single_bucket(
    dashboardid: Annotated[str, typer.Argument(help="The Dashboard ID to Email")],
    supplier_number: Annotated[str, typer.Argument(help="The Supplier Name to Use")],
    bucket_name: Annotated[str, typer.Argument(help="Google Bucket name")],
    key_file: Annotated[str, typer.Argument(help="Local path to key json file")],
    schedule_type: Annotated[str, typer.Argument(help="852 or 867")],
):
    """
    Use for debugging write files to local-path instead of email
    """
    sdk_conn = SDKConnection(os.getenv("LOOKERSDK_BASE_URL"))
    sdk = sdk_conn.get_sdk_connection()

    try:
        storage_bucket = CloudStorage(bucket_name, key_file)
        try:
            downloader = DashboardDownloader(
                sdk=sdk,
                dashboard_id=dashboardid,
                dashboard_filters={
                    "Billing Date": "last week",
                    "Supplier Number": supplier_number,
                },
            )

            downloader.create_tasks()
            files = downloader.get_tasks_results()

            compressed_xlsx = create_zip_file(files)

            with open(f"{supplier_number}.zip", "wb") as f:  # use `wb` mode
                f.write(compressed_xlsx.getvalue())

            storage_bucket.upload_file(
                f"{schedule_type}/{supplier_number}_manual.zip",
                compressed_xlsx,
            )
            print(
                f"Report: Manual File for Supplier Number {supplier_number} was uploaded to {bucket_name}"
            )
        # pylint: disable=broad-exception-caught
        except Exception as e:
            print(
                f"Report: Manual File for Supplier Number {supplier_number} was not sent to uploaded"
            )
            print(e)
    except:
        print("Could not download dashboard")
        print(traceback.format_exc())
        raise


if __name__ == "__main__":
    app()
