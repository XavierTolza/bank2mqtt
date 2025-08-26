import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from .handler import Handler

class EmailHandler(Handler):
    """
    A handler to send transaction notifications via email (SMTP).
    """
    def __init__(self, smtp_host: str, smtp_port: int, smtp_user: str, smtp_pass: str, to_email: str, from_email: str = None):
        """
        Initializes the Email handler.
        """
        self.smtp_config = {
            "host": smtp_host,
            "port": smtp_port,
            "user": smtp_user,
            "pass": smtp_pass
        }
        self.to_email = to_email
        self.from_email = from_email if from_email else smtp_user

    def process_transaction(self, data: dict) -> None:
        """
        Sends the transaction data as a formatted email.
        """
        transaction = data.get("transaction", {})
        account = data.get("account", {})

        subject = f"New Transaction Notification on account {account.get('name', '')}"
        
        body = f"A new transaction has been recorded:\n\n"
        body += f"  - Account: {account.get('name', 'N/A')}\n"
        body += f"  - Description: {transaction.get('description', 'N/A')}\n"
        body += f"  - Amount: {transaction.get('amount', 'N/A')} {account.get('currency', '')}\n"
        body += f"  - Date: {transaction.get('date', 'N/A')}\n\n"
        body += "---\nRaw JSON data:\n"
        body += json.dumps(data, indent=2, ensure_ascii=False)

        msg = MIMEMultipart()
        msg["From"] = self.from_email
        msg["To"] = self.to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(self.smtp_config["host"], self.smtp_config["port"]) as server:
                server.starttls()
                server.login(self.smtp_config["user"], self.smtp_config["pass"])
                server.send_message(msg)
                print(f"Successfully sent email to {self.to_email}")
        except Exception as e:
            print(f"Error sending email: {e}")

