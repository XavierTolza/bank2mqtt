"""
Pytest configuration and fixtures for bank2mqtt tests.
"""
import os
import tempfile
from pathlib import Path
from typing import Generator, Dict, Any
import pytest
import docker
from sqlalchemy import text
from bank2mqtt.db import Bank2MQTTDatabase, Base
from bank2mqtt.config import Config


class PostgresContainer:
    """Helper class to manage PostgreSQL container for testing."""

    def __init__(self):
        self.container = None
        self.client = docker.from_env()
        self.port = 5432
        self.user = "testuser"
        self.password = "testpassword"
        self.database = "bank2mqtt_test"

    def start(self) -> str:
        """Start PostgreSQL container and return connection URL."""
        # Remove any existing test container
        try:
            existing = self.client.containers.get("bank2mqtt_test_db")
            existing.stop()
            existing.remove()
        except docker.errors.NotFound:
            pass

        # Start new container
        self.container = self.client.containers.run(
            "postgres:15-alpine",
            name="bank2mqtt_test_db",
            environment={
                "POSTGRES_USER": self.user,
                "POSTGRES_PASSWORD": self.password,
                "POSTGRES_DB": self.database,
            },
            ports={"5432/tcp": 0},  # Bind to random available port
            detach=True,
            remove=False,
        )

        # Get the actual bound port
        self.container.reload()
        port_bindings = self.container.ports.get("5432/tcp")
        if port_bindings:
            self.port = int(port_bindings[0]["HostPort"])

        # Wait for container to be ready
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                import psycopg2

                conn = psycopg2.connect(
                    host="localhost",
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                )
                conn.close()
                break
            except psycopg2.OperationalError:
                if attempt == max_attempts - 1:
                    raise
                import time

                time.sleep(0.5)

        return f"postgresql://{self.user}:{self.password}@localhost:{self.port}/{self.database}"

    def stop(self):
        """Stop and remove PostgreSQL container."""
        if self.container:
            try:
                self.container.stop()
                self.container.remove()
            except Exception:
                pass
        self.container = None


@pytest.fixture
def temp_sqlite_db() -> Generator[Bank2MQTTDatabase, None, None]:
    """
    Fixture providing a temporary SQLite database for testing.
    Automatically creates tables and cleans up after the test.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db_url = f"sqlite:///{db_path}"

        db = Bank2MQTTDatabase(url=db_url)

        yield db

        # Cleanup
        db.close() if hasattr(db, 'close') else None


@pytest.fixture
def postgres_db() -> Generator[Bank2MQTTDatabase, None, None]:
    """
    Fixture providing a PostgreSQL database in a Docker container for testing.
    Automatically creates tables and cleans up after the test.
    """
    pg_container = PostgresContainer()
    db_url = pg_container.start()

    db = Bank2MQTTDatabase(url=db_url)

    yield db

    # Cleanup
    db.close() if hasattr(db, 'close') else None
    pg_container.stop()


@pytest.fixture(params=["sqlite"])
def test_db(
    request, temp_sqlite_db
) -> Generator[Bank2MQTTDatabase, None, None]:
    """
    Parametrized fixture that provides SQLite database for testing.
    Can be extended with PostgreSQL when schema issues are fixed.
    """
    if request.param == "sqlite":
        yield temp_sqlite_db
    else:
        # PostgreSQL currently not supported due to schema issues
        # Will be enabled after fixing domain_id type in Authentication model
        pass


@pytest.fixture
def mock_env(monkeypatch, test_db: Bank2MQTTDatabase) -> Dict[str, str]:
    """
    Fixture setting up environment variables for testing.
    Uses the test database URL and mock Powens credentials.
    """
    env_vars = {
        "BANK2MQTT_DB_URL": test_db.url,
        "MQTT_BROKER": "localhost",
        "MQTT_PORT": "1883",
        "MQTT_USER": "testuser",
        "MQTT_PASSWORD": "testpass",
        "POWENS_DOMAIN": "mock",
        "POWENS_CLIENT_ID": "test_client_id",
        "POWENS_CLIENT_SECRET": "test_client_secret",
        "POWENS_AUTH_TOKEN": "test_auth_token",
        "POWENS_REDIRECT_URI": "http://localhost:3000/callback",
        "SLEEP_INTERVAL": "60",
    }

    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    return env_vars


@pytest.fixture
def test_config(mock_env) -> Config:
    """
    Fixture providing a Config instance with test environment variables.
    """
    # Clear any cached imports to ensure fresh config
    import importlib
    import bank2mqtt.config

    importlib.reload(bank2mqtt.config)

    return Config.from_env()


@pytest.fixture
def sample_accounts() -> list[Dict[str, Any]]:
    """
    Fixture providing sample bank account data for testing.
    """
    return [
        {
            "id": 1,
            "id_connection": 1,
            "id_user": 1,
            "id_source": None,
            "id_parent": None,
            "number": "FR1420041010050500013M02606",
            "original_name": "Compte Chèques",
            "balance": 1500.50,
            "coming": 200.00,
            "display": True,
            "deleted": None,
            "disabled": None,
            "iban": "FR1420041010050500013M02606",
            "type": "Checking",
            "id_type": 1,
            "bookmarked": 0,
            "name": "Compte Chèques",
            "error": None,
            "usage": "PRIV",
            "ownership": "OWNER",
            "company_name": None,
            "loan": None,
            "last_update": "2024-01-15T10:00:00",
        },
        {
            "id": 2,
            "id_connection": 1,
            "id_user": 1,
            "id_source": None,
            "id_parent": None,
            "number": "FR1420041010150500013M02607",
            "original_name": "Compte Épargne",
            "balance": 5000.00,
            "coming": 0.00,
            "display": True,
            "deleted": None,
            "disabled": None,
            "iban": "FR1420041010150500013M02607",
            "type": "Savings",
            "id_type": 2,
            "bookmarked": 1,
            "name": "Compte Épargne",
            "error": None,
            "usage": "PRIV",
            "ownership": "OWNER",
            "company_name": None,
            "loan": None,
            "last_update": "2024-01-15T10:00:00",
        },
    ]
