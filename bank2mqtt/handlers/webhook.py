
import requests
import json
from .handler import Handler

class WebhookHandler(Handler):
    """
    A handler to send transaction data to a generic webhook URL.
    """
    def __init__(self, url: str):
        """
        Initializes the Webhook handler.

        Args:
            url (str): The webhook URL to send the POST request to.
        """
        if not url:
            raise ValueError("Webhook URL cannot be empty.")
        self.url = url

    def process_transaction(self, data: dict) -> None:
        """
        Sends the transaction data as a JSON payload to the webhook URL.
        """
        try:
            response = requests.post(
                self.url,
                json=data,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            print(f"Successfully sent data to webhook: {self.url}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending data to webhook: {e}")

