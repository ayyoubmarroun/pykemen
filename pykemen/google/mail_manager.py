"""Mail Manager module.

This module have a class with all needed functionalites
of Mail that AudienceScore needs.
"""
__author__ = 'Metriplica-Ayyoub&Javier'

import base64
import pykemen.utilities as utilities
from email.mime.text import MIMEText
from apiclient import errors  # noqa


class Mail(object):
    """Mail class.

    Useful to send messages form the specified account.
    """

    def __init__(self, secrets, credentials):
        """Init module initialize and create Mail class.

        Args:
            credentials (str): Credentials to access to the client services
            secrets (str): Secrets of the Google accout to use

        Returns:
            Mail: with given configuration.
        """
        scopes = scopes = [
            "https://www.googleapis.com/auth/gmail.send", 
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/bigquery.insertdata",
            ]
        self._gmailService = utilities.create_api('gmail', 'v1', scopes, secrets, credentials)

    def _createMessage(self, to, subject, body, type='plain'):
        """Create a message object."""
        message = MIMEText(body, type)
        message['to'] = ','.join(to)
        message['from'] = 'me'
        message['subject'] = subject
        return {'raw': base64.urlsafe_b64encode(message.as_string())}

    def sendMessage(self, to, subject, message, type='plain'):
        """Send a message to specified reciver.

        Args:
            to (list): Recivers of the email separated by comma
            subject (str): subject of the email
            message (str): message or body of the email

        Returns:
            int: message id.

        """
        try:
            objectMessage = self._createMessage(to, subject, message, type)
            messageId = self._gmailService.users().messages().send(
                userId='me', body=objectMessage).execute()
            return messageId
        except errors.HttpError as error:
            raise Exception(error)
