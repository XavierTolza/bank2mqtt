import base64
from functools import cached_property
import os
import requests
from typing import Optional, Dict, Any, List
import time

from loguru import logger


class PowensClient:
    """
    Client for interacting with Powens Banking API.
    """

    def __init__(
        self,
        domain: str,
        client_id: str,
        client_secret: str,
        auth_token: Optional[str] = None,
        callback_url: Optional[str] = None,
    ):
        logger.debug(f"Initializing PowensClient for domain: {domain}")
        self.base_url = f"https://{domain}.biapi.pro/2.0"

        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
        self.callback_url = callback_url
        self.auth_token = auth_token

    def get_new_auth_token(self) -> str:
        """
        Retrieve a permanent auth token for the app.
        """
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        logger.debug(f"Client ID: {self.client_id[:8]}...")

        resp = self._make_request(
            method="POST",
            endpoint="/auth/init",
            json_data=payload,
            requires_auth=False,
        )
        data = resp.json()

        token = data.get("auth_token")
        if not token:
            logger.error("No auth_token found in authentication response")
            raise ValueError("No auth_token in response")

        logger.success("Authentication completed successfully")
        logger.debug(f"Token length: {len(token)} characters")
        return token

    def get_temp_code(self) -> str:
        """
        Exchange the permanent token for a one-time code.
        """
        logger.info("Requesting temporary code")

        resp = self._make_request(
            method="GET",
            endpoint="/auth/token/code",
        )

        code = resp.json().get("code")
        if not code:
            logger.error("No code found in temporary code response")
            raise ValueError("No code in response")

        logger.success("Temporary code generated successfully")
        logger.debug(f"Code length: {len(code)} characters")
        return code

    # TODO cache timeout
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

        endpoint = "/users/me/accounts"
        params = {"all": ""} if all_accounts else None

        resp = self._make_request(
            method="GET",
            endpoint=endpoint,
            params=params,
        )

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

    def activate_account(self, account_id: int) -> Dict[str, Any]:
        """
        Activate a disabled account (grant user consent).
        """
        logger.info(f"Activating account {account_id}")

        endpoint = f"/users/me/accounts/{account_id}"
        params = {"all": ""}
        payload = {"disabled": False}

        logger.debug(f"Payload: {payload}")

        resp = self._make_request(
            method="POST",
            endpoint=endpoint,
            params=params,
            json_data=payload,
        )

        result = resp.json()
        logger.success(f"Account {account_id} activated successfully")
        return result

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

        if account_id:
            endpoint = f"/users/me/accounts/{account_id}/transactions"
            logger.debug(f"Fetching transactions for specific account: {account_id}")
        else:
            endpoint = "/users/me/transactions"
            logger.debug("Fetching transactions across all accounts")

        params: Dict[str, Any] = {"limit": limit, **kwargs}
        if date_from:
            params["start_date"] = date_from
        if date_to:
            params["end_date"] = date_to

        transactions = []

        try:
            # First request
            resp = self._make_request(
                method="GET",
                endpoint=endpoint,
                params=params,
            )

            result = resp.json()
            transactions.extend(result.get("transactions", []))
            next_url = (result["_links"].get("next", {}) or {}).get("href")

            # Follow pagination links
            while next_url and (limit is None or len(transactions) < limit):
                resp = self._make_request(
                    method="GET",
                    endpoint="",
                    full_url=next_url,
                )

                result = resp.json()
                transactions.extend(result.get("transactions", []))
                next_url = (result["_links"].get("next", {}) or {}).get("href")

            transaction_count = len(transactions)
            logger.success(f"Retrieved {transaction_count} transactions")

            transactions = sorted(transactions, key=lambda x: x["date"])
            if limit is not None:
                transactions = transactions[:limit]

            return transactions

        except requests.HTTPError as e:
            error_msg = f"Error fetching transactions: {e}"
            logger.error(error_msg)
            raise requests.HTTPError(error_msg) from e

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        requires_auth: bool = True,
        full_url: Optional[str] = None,
    ) -> requests.Response:
        """
        Centralized method for making HTTP requests with retry mechanism.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (without base URL)
            params: URL parameters
            json_data: JSON payload for POST requests
            headers: Additional headers
            requires_auth: Whether to add Authorization header
            full_url: Use this URL instead of building from base_url + endpoint

        Returns:
            requests.Response object
        """
        if requires_auth:
            self._ensure_authenticated()

        # Build URL
        if full_url:
            url = full_url
        else:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Build headers
        request_headers = {}
        if requires_auth and self.auth_token:
            request_headers["Authorization"] = f"Bearer {self.auth_token}"
        if json_data:
            request_headers["Content-Type"] = "application/json"
        if headers:
            request_headers.update(headers)

        # Log request details
        logger.debug(f"{method.upper()} {url}")
        if params:
            logger.debug(f"Request parameters: {params}")
        if json_data:
            logger.debug(f"Request payload: {json_data}")

        # Retry mechanism: 3 attempts with 60s timeout
        max_retries = 3
        timeout = 60  # 1 minute timeout

        for attempt in range(max_retries):
            try:
                logger.debug(f"Request attempt {attempt + 1}/{max_retries}")
                resp = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=request_headers,
                    timeout=timeout,
                )
                resp.raise_for_status()
                logger.debug(f"Response status: {resp.status_code}")
                if attempt > 0:
                    logger.info(f"Request succeeded on attempt {attempt + 1}")
                return resp

            except (requests.RequestException, requests.Timeout) as e:
                is_last_attempt = attempt == max_retries - 1
                if is_last_attempt:
                    error_msg = (
                        f"{method.upper()} request to {url} failed after "
                        f"{max_retries} attempts: {e}"
                    )
                    logger.error(error_msg)
                    raise
                else:
                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                    warning_msg = (
                        f"{method.upper()} request to {url} failed on "
                        f"attempt {attempt + 1}: {e}"
                    )
                    logger.warning(warning_msg)
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)

        # This should never be reached due to the raise in the last attempt
        raise RuntimeError("Unexpected end of retry loop")

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
        auth_token = base64.b64decode(os.getenv("POWENS_AUTH_TOKEN_B64", "")).decode(
            "utf-8"
        ) or os.getenv("POWENS_AUTH_TOKEN")

        logger.debug(f"Environment variables - Domain: {domain}")
        logger.debug(f"Client ID: {client_id[:8] + '...' if client_id else 'Not set'}")
        logger.debug(f"Client secret: {'Set' if client_secret else 'Not set'}")
        logger.debug(f"Callback URL: {callback_url or 'Not set'}")
        logger.debug(f"Auth token: {'Set' if auth_token else 'Not set'}")

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

        # Check no None values
        if None in [domain, client_id, client_secret]:
            raise ValueError("Domain, client_id, and client_secret must not be None")

        logger.success("Successfully created PowensClient from environment")
        return cls(
            domain=domain,  # pyright: ignore[reportArgumentType]
            client_id=client_id,  # pyright: ignore[reportArgumentType]
            client_secret=client_secret,  # pyright: ignore[reportArgumentType]
            callback_url=callback_url,
            auth_token=auth_token,
        )  # type: ignore
