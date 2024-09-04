import io
from google.cloud import storage
from google.api_core.exceptions import NotFound, GoogleAPICallError


class CloudStorage:
    def __init__(self, bucket_name: str, key_file: str) -> None:
        self.__client = None
        self.__bucket_name = bucket_name
        self.__key_file = key_file
        self.__bucket = None
        self.__get_client()
        self.__get_bucket()

    def __get_client(self):
        self.__client = storage.Client.from_service_account_json(self.__key_file)

    def __get_bucket(self):
        if self.__bucket is None:
            # try:
                self.__bucket = self.__client.get_bucket(self.__bucket_name)
                print(f"Bucket {self.__bucket_name} already exists.")
            # except NotFound:
            #     try:
            #         self.__bucket = self.__client.create_bucket(self.__bucket_name)
            #         print(f"Bucket {self.__bucket_name} created.")
            #     except GoogleAPICallError as e:
            #         print(f"Failed to create bucket {self.__bucket_name}: {e}")
            #         raise

    def upload_file(self, file_path: str, file_obj: io.BytesIO):
        """Upload a file to the Bucket Client

        Args:
            file_path (str): file path for the uploaded file

        Raises:
            e: Exception
        """
        try:
            blob = self.__bucket.blob(file_path)
            blob.upload_from_file(file_obj, rewind=True)
        except Exception as e:
            raise e
