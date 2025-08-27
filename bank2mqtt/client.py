from functools import cached_property
import os
import requests
from typing import Optional, Dict, Any, List, Generator
from datetime import datetime, timedelta

from bank2mqtt.cache import Cache
from loguru import logger
import hashlib


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
        logger.debug(f"Initializing PowensClient for domain: {domain}")
        self.base_url = f"https://{domain}.biapi.pro/2.0"
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url

        hash_input = f"{client_id}:{client_secret}".encode("utf-8")
        self.cache_key = hashlib.md5(hash_input).hexdigest()
        self.cache = Cache(domain, self.cache_key)
        self.auth_token = None

        logger.debug(f"Base URL: {self.base_url}")
        logger.debug(f"Cache key: {self.cache_key[:8]}...")
        logger.debug(f"Callback URL: {callback_url or 'Not set'}")

    def authenticate(self) -> str:
        """
        Retrieve a permanent auth token for the app.
        """
        logger.info("Starting authentication process")

        if "authenticate" in self.cache:
            logger.debug("Using cached authentication data")
            data = self.cache["authenticate"]
        else:
            logger.debug("Performing fresh authentication request")
            url = f"{self.base_url}/auth/init"
            payload = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

            logger.debug(f"Authentication URL: {url}")
            logger.debug(f"Client ID: {self.client_id[:8]}...")

            try:
                resp = requests.post(url, json=payload)
                resp.raise_for_status()
                logger.debug(f"Authentication response status: {resp.status_code}")
                data = resp.json()
                self.cache["authenticate"] = data
                logger.info("Authentication data cached successfully")
            except requests.RequestException as e:
                logger.error(f"Authentication request failed: {e}")
                raise

        token = data.get("auth_token")
        if not token:
            logger.error("No auth_token found in authentication response")
            raise ValueError("No auth_token in response")

        self.auth_token = token
        logger.success("Authentication completed successfully")
        logger.debug(f"Token length: {len(token)} characters")
        return token

    def get_temp_code(self) -> str:
        """
        Exchange the permanent token for a one-time code.
        """
        logger.info("Requesting temporary code")
        self._ensure_authenticated()

        url = f"{self.base_url}/auth/token/code"
        headers = {"Authorization": f"Bearer {self.auth_token}"}

        logger.debug(f"Temporary code URL: {url}")

        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            logger.debug(f"Temporary code response status: {resp.status_code}")

            code = resp.json().get("code")
            if not code:
                logger.error("No code found in temporary code response")
                raise ValueError("No code in response")

            logger.success("Temporary code generated successfully")
            logger.debug(f"Code length: {len(code)} characters")
            return code

        except requests.RequestException as e:
            logger.error(f"Temporary code request failed: {e}")
            raise

    @cached_property
    def temp_code(self):
        return self.get_temp_code()

    def get_webview_url(self, lang: str = "fr", flow: str = "manage", **kwargs) -> str:
        """
        Build the Powens Connect Webview URL.
        """
        logger.info(f"Generating webview URL (lang={lang}, flow={flow})")

        params = {
            "domain": self.base_url.split("//")[1].split(".")[0],
            "client_id": self.client_id,
            "code": self.temp_code,
            **kwargs,
        }
        if self.callback_url is not None:
            params["redirect_uri"] = self.callback_url

        from urllib.parse import urlencode

        url = f"https://webview.powens.com/{lang}/{flow}?{urlencode(params)}"
        logger.debug(f"Generated webview URL: {url}")
        logger.success("Webview URL generated successfully")

        return url

    def list_accounts(self, all_accounts: bool = False) -> List[Dict[str, Any]]:
        """
        List user bank accounts. If all_accounts=True, include disabled.
        """
        logger.info(f"Listing accounts (include_disabled={all_accounts})")
        self._ensure_authenticated()

        url = f"{self.base_url}/users/me/accounts"
        if all_accounts:
            url += "?all"

        headers = {"Authorization": f"Bearer {self.auth_token}"}
        logger.debug(f"Accounts URL: {url}")

        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            logger.debug(f"Accounts response status: {resp.status_code}")

            accounts = resp.json().get("accounts", [])
            logger.success(f"Retrieved {len(accounts)} accounts")

            # Log account details
            for account in accounts:
                account_id = account.get("id", "unknown")
                account_name = account.get("name", "unknown")
                disabled = account.get("disabled", False)
                status = "disabled" if disabled else "active"
                logger.debug(f"Account {account_id}: {account_name} ({status})")

            return accounts

        except requests.RequestException as e:
            logger.error(f"Failed to list accounts: {e}")
            raise

    def activate_account(self, account_id: int) -> Dict[str, Any]:
        """
        Activate a disabled account (grant user consent).
        """
        logger.info(f"Activating account {account_id}")
        self._ensure_authenticated()

        url = f"{self.base_url}/users/me/accounts/{account_id}?all"
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }
        payload = {"disabled": False}

        logger.debug(f"Account activation URL: {url}")
        logger.debug(f"Payload: {payload}")

        try:
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            logger.debug(f"Account activation response status: {resp.status_code}")

            result = resp.json()
            logger.success(f"Account {account_id} activated successfully")
            return result

        except requests.RequestException as e:
            logger.error(f"Failed to activate account {account_id}: {e}")
            raise

    def list_transactions(
        self,
        account_id: Optional[int] = None,
        limit: Optional[int] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        List transactions. If account_id is None, return across all connections.
        """
        logger.info(
            f"Listing transactions (account_id={account_id}, limit={limit}, "
            f"date_from={date_from}, date_to={date_to})"
        )
        self._ensure_authenticated()

        if account_id:
            path = f"accounts/{account_id}/transactions"
            logger.debug(f"Fetching transactions for specific account: {account_id}")
        else:
            path = "transactions"
            logger.debug("Fetching transactions across all accounts")

        params: Dict[str, Any] = {"limit": limit, **kwargs}
        if date_from:
            params["start_date"] = date_from
        if date_to:
            params["end_date"] = date_to

        from urllib.parse import urlencode

        url = f"{self.base_url}/users/me/{path}?{urlencode(params)}"
        headers = {"Authorization": f"Bearer {self.auth_token}"}

        logger.debug(f"Transactions URL: {url}")
        logger.debug(f"Request parameters: {params}")

        transactions = []
        try:
            while url:
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                logger.debug(f"Transactions response status: {resp.status_code}")

                result = resp.json()
                transactions.extend(result.get("transactions", []))
                url = (result["_links"].get("next", {}) or {}).get("href")

            transaction_count = len(transactions)
            logger.success(f"Retrieved {transaction_count} transactions")

            transactions = sorted(transactions, key=lambda x: x["date"])
            return transactions

        except requests.HTTPError as e:
            error_msg = f"Error fetching transactions: {e}"
            try:
                error_msg += f" - Response: {resp.text}"
            except (NameError, AttributeError):
                pass
            logger.error(error_msg)
            raise requests.HTTPError(error_msg) from e

    def _ensure_authenticated(self):
        if not self.auth_token:
            logger.error("Client is not authenticated - no auth token available")
            raise RuntimeError(
                "Client is not authenticated. Call authenticate() first."
            )
        logger.debug("Authentication check passed")

    @classmethod
    def from_env(cls) -> "PowensClient":
        logger.info("Creating PowensClient from environment variables")

        domain = os.getenv("POWENS_DOMAIN")
        client_id = os.getenv("POWENS_CLIENT_ID")
        client_secret = os.getenv("POWENS_CLIENT_SECRET")
        callback_url = os.getenv("POWENS_CALLBACK_URL")

        logger.debug(f"Environment variables - Domain: {domain}")
        logger.debug(f"Client ID: {client_id[:8] + '...' if client_id else 'Not set'}")
        logger.debug(f"Client secret: {'Set' if client_secret else 'Not set'}")
        logger.debug(f"Callback URL: {callback_url or 'Not set'}")

        if not all([domain, client_id, client_secret]):
            missing_vars = []
            if not domain:
                missing_vars.append("POWENS_DOMAIN")
            if not client_id:
                missing_vars.append("POWENS_CLIENT_ID")
            if not client_secret:
                missing_vars.append("POWENS_CLIENT_SECRET")

            logger.error(f"Missing required environment variables: {missing_vars}")
            raise ValueError(
                "Environment variables POWENS_DOMAIN, POWENS_CLIENT_ID, and "
                "POWENS_CLIENT_SECRET must be set"
            )

        logger.success("Successfully created PowensClient from environment")
        return cls(domain, client_id, client_secret, callback_url)  # type: ignore
