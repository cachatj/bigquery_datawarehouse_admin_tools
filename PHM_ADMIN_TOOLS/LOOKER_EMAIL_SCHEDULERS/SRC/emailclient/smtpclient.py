import smtplib
import os
from io import BytesIO
import base64
import dotenv

from email.message import EmailMessage
from email.utils import formatdate

dotenv.load_dotenv()

class SmtpClient:
    def __init__(self) -> None:
        self.__client = None
        self.__get_client()

    def __get_client(self):
        try:
            print(f"Using SMTP Server {os.getenv("SMTP_HOST")}")
            self.__client = smtplib.SMTP(os.getenv("SMTP_HOST"))
        except Exception as e:
            raise e

    def send_message_with_zip_report(
        self,
        destination: list[str],
        mail_cc: str,
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
            dict: returns a success message if needed
        """
        msg = EmailMessage()
        msg["From"] = "GMB-DUB-DCOE@cardinalhealth.com"
        msg["Subject"] = subject
        if isinstance(destination, list):
            msg["To"] = ','.join(destination)
        elif isinstance(destination, str):
            msg["To"]=destination
        else:
            raise Exception("Invalid to Email")
        msg["Cc"] = mail_cc
        msg['Date'] = formatdate(localtime=True)
        msg.set_content(content)

        # encoded = base64.b64encode(attachment.getvalue()).decode()
        attachment.seek(0)

        msg.add_attachment(
            attachment.getvalue(),
            maintype="application",
            subtype="zip",
            filename=filename,
        )

        try:
            # TODO YOU DO NOT NEED TO LOGIN
            print(f"Sending to {destination}")
            # self.__client.starttls()
            print(f"SMTP USER NAME: {os.getenv('SMTP_USER_NAME')}")
            self.__client.login(os.getenv("SMTP_USER_NAME"), os.getenv("SMTP_PASSWORD"))
            self.__client.send_message(msg)
            return {"success": True, "msg": "Email Sent out Successfully"}
        # pylint: disable=broad-exception-caught
        except Exception as e:
            print(e)
            print("Could not send message to %s", destination)
            return {"success": False, "msg": f"Could not send email to {destination}"}
        
    def send_test_message(
            self,
            from_address: str, 
            destination: list[str],
            subject: str,
            content: str,
    ) -> dict:
        msg = EmailMessage()
        msg["From"]=from_address
        msg['To']=','.join(destination)
        msg['Subject']=subject
        msg['Date'] = formatdate(localtime=True)
        msg.set_content(content)

        try:
            print(f"Sending to {destination}")
            self.__client.starttls()
            #self.__client.login("f82cf361050dd1", "ddff3481ebaa92")
            self.__client.send_message(msg)
            return {"success": True, "msg": "Email Sent out Successfully"}
        # pylint: disable=broad-exception-caught
        except Exception as e:
            print(e)
            print("Could not send message to {destination}")
            return {"success": False, "msg": f"Could not send email to {destination}"}


if __name__ == "__main__":
    stmpclient=SmtpClient()
    stmpclient.send_test_message(from_address="test@email.com", destination=['fernando.rodriguez@bytecode.io'],subject='Test Email',content='Content of the Email')
