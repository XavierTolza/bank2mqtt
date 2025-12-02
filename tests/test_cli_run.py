"""
Integration tests for the bank2mqtt CLI run command.
Tests the full flow of fetching accounts and saving them to the database.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from bank2mqtt.db import Bank2MQTTDatabase, Account


class TestCliRun:
    """Test suite for the CLI run command."""

    @pytest.fixture
    def mock_client(self, sample_accounts):
        """
        Fixture providing a mock Powens client that returns sample accounts.
        """
        client = Mock()
        client.get_webview_url.return_value = "https://mock.example.com/webview"
        client.list_accounts.return_value = sample_accounts
        client.list_transactions.return_value = []
        client.cache.close.return_value = None
        return client

    @pytest.fixture
    def mock_mqtt(self):
        """
        Fixture providing a mock MQTT handler.
        """
        mqtt = MagicMock()
        mqtt.__enter__ = Mock(return_value=mqtt)
        mqtt.__exit__ = Mock(return_value=None)
        mqtt.process_transaction.return_value = None
        return mqtt

    def _run_command_once(
        self, test_db: Bank2MQTTDatabase, mock_client, mock_mqtt
    ):
        """
        Helper method to run the 'run' command once.
        Patches the global client and mqtt objects to use mocks.
        """
        import bank2mqtt.__main__ as main_module
        import sys

        # Patch Config.from_env to avoid loading env vars before import
        with patch("bank2mqtt.config.Config.from_env") as mock_config_from_env:
            mock_config = Mock()
            mock_config.db = test_db
            mock_config.client = mock_client
            mock_config.mqtt_handler = mock_mqtt
            mock_config.sleep_interval = 1
            mock_config_from_env.return_value = mock_config

            # Now import run
            if "bank2mqtt.__main__" in sys.modules:
                del sys.modules["bank2mqtt.__main__"]

            from bank2mqtt.__main__ import run

            # Patch the global objects
            with patch.object(main_module, "client", mock_client):
                with patch.object(main_module, "mqtt", mock_mqtt):
                    with patch.object(main_module, "db", test_db):
                        # Create an iterator that yields once then stops
                        call_count = {"count": 0}

                        def side_effect(*args, **kwargs):
                            call_count["count"] += 1
                            if call_count["count"] > 1:
                                raise KeyboardInterrupt()
                            return

                        with patch("time.sleep", side_effect=side_effect):
                            try:
                                run()
                            except KeyboardInterrupt:
                                pass

    def test_run_saves_accounts_to_database(
        self, test_db, mock_client, mock_mqtt, sample_accounts
    ):
        """
        Test that the run command fetches accounts and saves them to the db.
        """
        self._run_command_once(test_db, mock_client, mock_mqtt)

        # Verify accounts were saved
        with test_db.session_scope() as session:
            accounts = session.query(Account).all()

        assert len(accounts) == len(sample_accounts)
        assert len(accounts) > 1, "Should have more than one account"

        # Verify account details
        account_ids = {acc.id for acc in accounts}
        assert account_ids == {1, 2}

        # Verify specific account data
        account_1 = next(acc for acc in accounts if acc.id == 1)
        assert account_1.name == "Compte Chèques"
        assert account_1.iban == "FR1420041010050500013M02606"
        assert float(account_1.balance) == 1500.50

        account_2 = next(acc for acc in accounts if acc.id == 2)
        assert account_2.name == "Compte Épargne"
        assert account_2.iban == "FR1420041010150500013M02607"
        assert float(account_2.balance) == 5000.00

    def test_run_calls_client_methods(self, test_db, mock_client, mock_mqtt):
        """
        Test that the run command calls the expected client methods.
        """
        self._run_command_once(test_db, mock_client, mock_mqtt)

        # Verify client methods were called
        mock_client.get_webview_url.assert_called()
        mock_client.list_accounts.assert_called()
        mock_client.list_transactions.assert_called()

    def test_run_with_empty_accounts(self, test_db, mock_client, mock_mqtt):
        """
        Test that the run command handles the case where no accounts are found.
        """
        mock_client.list_accounts.return_value = []

        self._run_command_once(test_db, mock_client, mock_mqtt)

        # Verify no accounts were saved
        with test_db.session_scope() as session:
            accounts = session.query(Account).all()

        assert len(accounts) == 0

    def test_accounts_persisted_across_sessions(
        self, test_db, mock_client, mock_mqtt, sample_accounts
    ):
        """
        Test that accounts saved in one session persist in another.
        """
        # First run - save accounts
        self._run_command_once(test_db, mock_client, mock_mqtt)

        # Verify accounts exist
        with test_db.session_scope() as session:
            accounts_first = session.query(Account).all()
        assert len(accounts_first) == len(sample_accounts)

        # Second run - should not duplicate accounts
        self._run_command_once(test_db, mock_client, mock_mqtt)

        # Verify accounts still exist (no duplicates)
        with test_db.session_scope() as session:
            accounts_second = session.query(Account).all()
        assert len(accounts_second) == len(sample_accounts)

    def test_run_updates_existing_accounts(self, test_db, mock_client, mock_mqtt):
        """
        Test that the run command updates account data when balances change.
        """
        sample_accounts_v1 = [
            {
                "id": 1,
                "id_connection": 1,
                "id_user": 1,
                "id_source": None,
                "id_parent": None,
                "number": "FR1420041010050500013M02606",
                "original_name": "Compte Chèques",
                "balance": 1000.00,
                "coming": 0.00,
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
            }
        ]

        mock_client.list_accounts.return_value = sample_accounts_v1
        self._run_command_once(test_db, mock_client, mock_mqtt)

        with test_db.session_scope() as session:
            acc = session.query(Account).filter_by(id=1).first()
            assert float(acc.balance) == 1000.00

        # Second run with updated balance
        sample_accounts_v2 = [
            {
                **sample_accounts_v1[0],
                "balance": 1500.00,
                "last_update": "2024-01-15T11:00:00",
            }
        ]
        mock_client.list_accounts.return_value = sample_accounts_v2

        self._run_command_once(test_db, mock_client, mock_mqtt)

        with test_db.session_scope() as session:
            acc = session.query(Account).filter_by(id=1).first()
            assert float(acc.balance) == 1500.00

    @pytest.mark.parametrize("db_type", ["sqlite", "postgres"])
    def test_run_with_both_db_backends(
        self, db_type, mock_client, mock_mqtt, sample_accounts,
        temp_sqlite_db, postgres_db
    ):
        """
        Test that the run command works with both backends.
        """
        test_db = (
            temp_sqlite_db if db_type == "sqlite" else postgres_db
        )

        self._run_command_once(test_db, mock_client, mock_mqtt)

        with test_db.session_scope() as session:
            accounts = session.query(Account).all()

        assert len(accounts) == len(sample_accounts)
