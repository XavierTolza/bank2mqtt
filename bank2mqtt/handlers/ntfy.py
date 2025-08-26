import requests
from .handler import Handler


class NtfyHandler(Handler):
    """
    A handler to send transaction notifications to an NTFY topic.
    """

    def __init__(self, topic: str, server: str = "https://ntfy.sh"):
        """
        Initializes the NTFY handler.

        Args:
            topic (str): The NTFY topic to publish to.
            server (str, optional): The NTFY server URL. Defaults to "https://ntfy.sh".
        """
        if not topic:
            raise ValueError("NTFY topic cannot be empty.")
        self.url = f"{server}/{topic}"

    def process_transaction(self, data: dict) -> None:
        """
        Sends the transaction data as a notification to the NTFY topic.
        """
        transaction = data.get("transaction", {})
        account = data.get("account", {})

        title = f"New transaction on {account.get('name', 'account')}"
        message = (
            f"Amount: {transaction.get('amount')} {account.get('currency')}\n"
            f"Description: {transaction.get('description')}"
        )

        try:
            response = requests.post(
                self.url,
                headers={
                    "Title": title.encode("utf-8"),
                    "Content-Type": "text/plain; charset=utf-8",
                },
                data=message.encode("utf-8"),
            )
            response.raise_for_status()
            print(f"Successfully sent notification to NTFY topic: {self.url}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending notification to NTFY: {e}")
