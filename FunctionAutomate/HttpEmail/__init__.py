"""Send emails upon HTTP request.

Depends on the presence of an appropiate Key Vault to store passwords and
text file based DB stored in the storage account.

Requires the following env variables:
- KEY_VAULT_URI
- AzureWebJobsStorage

Author: Guillem Ballesteros
"""

import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Dict, List, Union

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.fileshare import ShareFileClient

from __app__.utilities import utilities


class SenderDB:
    """Setup our email accounts "DB", it is really just a JSON file.

    The sender details are obtained from a text file stored in an Azure
    File Share. To avoid having plain text password the text based DB stores
    a reference to a Key vault Secret.

    The connection to the Key Vault is established using the following
    environment defined variables:
    - AZURE_CLIENT_ID
    - AZURE_CLIENT_SECRET
    - AZURE_TENANT_ID
    The client ID and client secret are specific to an App registered in your
    active directory.

    The KEY_VAULT_URI is also expected to be found on the environment
    variables.

    The DB is a JSON file with a list of dicts that include:
    - user: The name which we will refer the account with in the requests.
    - email: Email of the senders.
    - host: Host for the SMTP server.
    - port: Port to the SMTP server.
    - keyvault_secret: The name of the secret stored in the KeyVault which has
        the password for the account.
    """

    def __init__(self, conn_str: str, share_name: str, file_path: str) -> None:
        """Initialize the sender class.

        Retrieves the DB from the file share. All the parameters of __init__
        are there to retrieve the DB.

        Parameters
        ----------
        conn_str
            Connection strin to the storage account containing the DB. Every
            Function App has an storage account associated with it. It's
            connection strin is stored in the default env variable
            AzureWebJobsStorage.
        share_name
            Name of the share where the DB is kept.
        file_path
            Path within the File Share to the DB.
        """

        file_client = ShareFileClient.from_connection_string(
            conn_str=conn_str, share_name=share_name, file_path=file_path,
        )

        data = file_client.download_file()
        self.email_db = json.loads(data.readall())

    def get_sender(self, user: str) -> Dict[str, Union[str, int]]:
        """Retrieve the details for a user from the DB.

        If we try to retrieve a user defined multiple times it raises an
        error since we have ambiguous details.

        Passwords are retrieved from a KeyVault.

        Parameters
        ----------
        user
            User associated with the email account used to deliver the email.
        """

        sender_details_lst = [x for x in self.email_db if x["user"] == user]
        if len(sender_details_lst) == 0:
            logging.info("Sender user not found in DB.")
            raise KeyError("Sender not found in DB.")
        elif len(sender_details_lst) > 1:
            logging.info("More than one sender user in DB. Please fix.")
            raise KeyError("Ambiguous sender found in DB")
        else:
            sender_details: Dict[str, Union[str, int]] = sender_details_lst[0]

        credential = DefaultAzureCredential()
        client = SecretClient(
            vault_url=os.environ["KEY_VAULT_URI"], credential=credential
        )
        secret = client.get_secret(sender_details["keyvault_secret"]).value
        sender_details["password"] = secret

        return sender_details


def parse_request(req: func.HttpRequest) -> Dict[str, Union[str, List[str]]]:
    """Extract all the relevant parameters from the incoming request.

    The parameters extracted are:
    - user (mandatory)
    - subject (optional default:empty)
    - recipients (mandatory): Comma separated list of recipiients.
    - body (optional default: empty)
    - mimetype (optional default: plain)
    """
    param_names = ["user", "subject", "recipients", "body", "mimetype"]
    email_parameters = {k: utilities.get_param(req, k) for k in param_names}
    logging.info(f"The incoming parameters are: {email_parameters}")

    # Further prep the parameters
    if not email_parameters["recipients"]:
        logging.info("Failed delivery. No recipients received.")
        raise KeyError("Email recipients not specified in request.")
    else:
        email_parameters["recipients"] = email_parameters["recipients"].split(",")

    if not email_parameters["user"]:
        logging.info("Failed delivery. No sender user specified.")
        raise KeyError("Sender user was not specified in request.")

    # Set the default parameters
    if not email_parameters["body"]:
        email_parameters["body"] = ""
    if not email_parameters["subject"]:
        email_parameters["subject"] = ""
    if not email_parameters["mimetype"]:
        email_parameters["mimetype"] = "plain"

    return email_parameters


class EmailDeliverer:
    """Configure and deliver emails."""

    def __init__(self, host: str, port: int, email: str, password: str) -> None:
        """Init email deliverer.

        Parameters
        ----------
        host
            Host for SMTP server.
        port
            SMTP port
        email
            Email messages are being delivered from.
        password
            Password to the email account.
        """
        self.host = host
        self.port = port
        self.password = password
        self.email = email

    def send_email(
        self, recipients: List[str], subject: str, body: str, mimetype: str
    ) -> None:
        """Send email.

        Only one message is attached to the email.

        Parameters
        ----------
        recipients
            List of emails who are going to receive the email.
        subject
            Subject line of the email.
        body
            Text body of the email.
        mimetype
            MIME type for the attached message
        """
        msg = MIMEMultipart()
        msg["From"] = self.email
        msg["To"] = ",".join(recipients)
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg.attach(MIMEText(body, mimetype))

        server = smtplib.SMTP(self.host, self.port)
        server.starttls()
        server.login(self.email, self.password)
        server.send_message(msg)
        server.quit()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure function to send emails triggered by HTTP request."""
    logging.info("Send email triggered via HTTP.")

    email_parameters = parse_request(req)

    sender_details = SenderDB(
        conn_str=os.environ["AzureWebJobsStorage"],
        share_name="email-app",
        file_path="emails.json",
    ).get_sender(str(email_parameters["user"]))

    postman = EmailDeliverer(
        host=str(sender_details["host"]),
        port=int(sender_details["port"]),
        email=str(sender_details["email"]),
        password=str(sender_details["password"]),
    )

    postman.send_email(
        recipients=list(email_parameters["recipients"]),
        subject=str(email_parameters["subject"]),
        body=str(email_parameters["body"]),
        mimetype=str(email_parameters["mimetype"]),
    )

    return func.HttpResponse("{}")
