from typing import Union
from os import getenv
from google.cloud import bigquery
import google.auth
import google.auth.impersonated_credentials
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator


class BigQuery:
    def __init__(self, project_id: str) -> None:
        """This class is used to get BigQuery connection

        Args:
            arguments (dict): Required arguments to connect to BigQuery
        """
        self.__client = None
        self.__key_file = None
        self.__project_id = project_id
        self.__get_connection()

    def __get_connection(self):
        """Get the BigQuery connection."""
        print(f"project_id: {self.__project_id}")
        impersonate_principal=getenv("SERVICE_ACCOUNT")
        print(f"impersonate_principal: {impersonate_principal}")
        target_scopes = [
            "https://www.googleapis.com/auth/devstorage.read_only",
            'https://www.googleapis.com/auth/bigquery'
        ]

        creds, pid = google.auth.default()

        if self.__project_id == "":
            self.__project_id = pid

        if self.__key_file is not None:
            self.__client = bigquery.Client.from_service_account_json(self.__key_file)
        elif impersonate_principal is not None:
            print(f"BQ client connection using {creds.service_account_email} impersonation.")
            tcreds = google.auth.impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=getenv("SERVICE_ACCOUNT"),
                target_scopes=target_scopes,
            )
            self.__client = bigquery.Client(credentials=tcreds,project=self.__project_id)
        else :
            self.__client = bigquery.Client(credentials=creds,project=self.__project_id)

    def read_data(self, query: str) -> Union["RowIterator", _EmptyRowIterator]:
        """Read data using the given query and return data in pandas DataFrame

        Args:
            query (str): Query to read

        Returns:
            RowIterator
        """
        return self.__client.query(query).result()
