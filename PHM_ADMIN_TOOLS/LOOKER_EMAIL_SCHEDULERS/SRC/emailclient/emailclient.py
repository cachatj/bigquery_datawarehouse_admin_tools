from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Attachment,
    ContentId,
    Disposition,
    FileContent,
    FileName,
    FileType,
    Mail,
    To,
)
import os
from io import BytesIO
import base64


class EmailClient:
    def __init__(self, send_grid_key: str) -> None:
        self.__send_grid_key = send_grid_key
        self.__client = None
        self.__get_client()

    def __get_client(self):
        try:
            self.__client = SendGridAPIClient(self.__send_grid_key)
        except Exception as e:
            raise e

    def send_message_with_zip_report(
        self,
        destination: list[str],
        subject: str,
        content: str,
        filename: str,
        attachment: BytesIO,
    ) -> dict:
        """Send a 

        Args:
            destination (list[str]): _description_
            subject (str): _description_
            content (str): _description_
            filename (str): _description_
            attachment (BytesIO): _description_

        Returns:
            dict: _description_
        """
        message = Mail(
            from_email="fernando.rodriguez@bytecode.io",
            to_emails=[To(dest) for dest in destination],
            subject=subject,
            html_content=content,
        )

        encoded = base64.b64encode(attachment.getvalue()).decode()
        attachment = Attachment()
        attachment.file_content = FileContent(encoded)
        attachment.file_type = FileType("application/zip")
        attachment.file_name = FileName(filename)
        attachment.disposition = Disposition("attachment")
        attachment.content_id = ContentId("Report_ID")
        message.attachment = attachment

        try:
            self.__client.send(message)
            return {"success": True, "msg": "Email Sent out Successfully"}
        except Exception:
            print("Could not send message to %s", destination)
            return {"success": False, "msg": f"Could not send email to {destination}"}
