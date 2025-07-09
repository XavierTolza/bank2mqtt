import os
import requests
from typing import Optional, Dict, Any, List


class PowensClient:
    """
    Client for interacting with Powens Banking API.
    """

    def __init__(
        self,
        domain: str,
        client_id: str,
        client_secret: str,
        callback_url: Optional[str] = None,
    ):
        self.base_url = f"https://{domain}.biapi.pro/2.0"
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url
        self.auth_token: Optional[str] = None

    def authenticate(self) -> str:
        """
        Retrieve a permanent auth token for the app.
        """
        url = f"{self.base_url}/auth/init"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = requests.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("auth_token")
        if not token:
            raise ValueError("No auth_token in response")
        self.auth_token = token
        return token

    def get_temp_code(self) -> str:
        """
        Exchange the permanent token for a one-time code.
        """
        self._ensure_authenticated()
        url = f"{self.base_url}/auth/token/code"
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        code = resp.json().get("code")
        if not code:
            raise ValueError("No code in response")
        return code

    def get_connect_url(self, code: str) -> str:
        """
        Build the Powens Connect Webview URL.
        """
        if not self.callback_url:
            raise ValueError("callback_url not set")
        params = {
            "domain": self.base_url.split("//")[1].split(".")[0],
            "client_id": self.client_id,
            "redirect_uri": self.callback_url,
            "code": code,
        }
        from urllib.parse import urlencode

        return f"https://webview.powens.com/connect?{urlencode(params)}"

    def list_accounts(self, all_accounts: bool = False) -> List[Dict[str, Any]]:
        """
        List user bank accounts. If all_accounts=True, include disabled.
        """
        self._ensure_authenticated()
        url = f"{self.base_url}/users/me/accounts"
        if all_accounts:
            url += "?all"
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json().get("accounts", [])

    def activate_account(self, account_id: int) -> Dict[str, Any]:
        """
        Activate a disabled account (grant user consent).
        """
        self._ensure_authenticated()
        url = f"{self.base_url}/users/me/accounts/{account_id}?all"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        payload = {"disabled": False}
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def list_transactions(
        self,
        account_id: Optional[int] = None,
        limit: int = 50,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List transactions. If account_id is None, return across all connections.
        """
        self._ensure_authenticated()
        if account_id:
            path = f"accounts/{account_id}/transactions"
        else:
            path = "transactions"
        params = {"limit": limit}
        if date_from:
            params["start_date"] = date_from
        if date_to:
            params["end_date"] = date_to
        from urllib.parse import urlencode

        url = f"{self.base_url}/users/me/{path}?{urlencode(params)}"
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def _ensure_authenticated(self):
        if not self.auth_token:
            raise RuntimeError(
                "Client is not authenticated. Call authenticate() first."
            )

    @classmethod
    def from_env(cls) -> "PowensClient":
        domain = os.getenv("POWENS_DOMAIN")
        client_id = os.getenv("POWENS_CLIENT_ID")
        client_secret = os.getenv("POWENS_CLIENT_SECRET")
        callback_url = os.getenv("POWENS_CALLBACK_URL")

        if not all([domain, client_id, client_secret]):
            raise ValueError(
                "Environment variables POWENS_DOMAIN, POWENS_CLIENT_ID, and "
                "POWENS_CLIENT_SECRET must be set"
            )

        return cls(domain, client_id, client_secret, callback_url)  # type: ignore
