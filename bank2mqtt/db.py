"""
SQLAlchemy database abstraction for bank2mqtt.

This module provides database models and session management for storing
bank account information, authentication tokens, and transaction data.
"""

import json
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, List, Callable
from sqlalchemy import (
    Boolean,
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.types import TypeDecorator, TEXT
from loguru import logger

Base = declarative_base()


def with_session(func: Callable) -> Callable:
    """
    Decorator to provide a database session to methods.

    The decorated method should accept 'session' as its first parameter
    after 'self'. The decorator handles session creation, error handling,
    and cleanup automatically.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.get_session() as session:
            try:
                return func(self, session, *args, **kwargs)
            except Exception as e:
                session.rollback()
                logger.error(f"Database operation failed in {func.__name__}: {e}")
                raise

    return wrapper


class JSONType(TypeDecorator):
    """A custom SQLAlchemy type for storing JSON data."""

    impl = TEXT
    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        """Convert Python object to JSON string for storage."""
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value: Optional[str], dialect) -> Any:
        """Convert JSON string back to Python object."""
        if value is not None:
            return json.loads(value)
        return value


class Account(Base):
    """Account table for storing bank account credentials."""

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(255), nullable=False)
    client_id = Column(String(255), nullable=False)
    client_secret = Column(String(255), nullable=False)

    # Unique constraint on domain and client_id combination
    __table_args__ = (
        UniqueConstraint("domain", "client_id", name="unique_domain_client"),
    )

    # Relationships
    authentications = relationship(
        "Authentication", back_populates="account", cascade="all, delete-orphan"
    )
    transactions = relationship(
        "Transaction", back_populates="account", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Account(id={self.id}, domain='{self.domain}', "
            f"client_id='{self.client_id}')>"
        )


class Authentication(Base):
    """Authentication table for storing auth tokens linked to accounts."""

    __tablename__ = "authentication"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    auth_token = Column(String(500), nullable=False, unique=True)
    register_date = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    account = relationship("Account", back_populates="authentications")

    def __repr__(self) -> str:
        return (
            f"<Authentication(id={self.id}, account_id={self.account_id}, "
            f"register_date={self.register_date})>"
        )


class Transaction(Base):
    """Transaction table for storing bank transaction data."""

    __tablename__ = "transactions"

    id = Column(String(255), primary_key=True)  # Unique transaction ID
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    register_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    transaction_date = Column(DateTime, nullable=False)
    powens_date = Column(DateTime, nullable=True)
    sent = Column(Boolean, default=False, nullable=False)
    data = Column(JSONType, nullable=True)

    # Relationships
    account = relationship("Account", back_populates="transactions")

    def __repr__(self) -> str:
        return (
            f"<Transaction(id='{self.id}', account_id={self.account_id}, "
            f"transaction_date={self.transaction_date})>"
        )


class DatabaseManager:
    """Database manager for handling SQLAlchemy sessions and operations."""

    def __init__(self, database_url: str):
        """
        Initialize the database manager.

        Args:
            database_url: SQLAlchemy database URL. If None, uses SQLite in
                         user data directory.
            app_name: Application name for the data directory.
        """
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

        # Create tables if they don't exist
        self.create_tables()

    def create_tables(self) -> None:
        """Create all database tables."""
        logger.info("Creating database tables...")
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()

    @with_session
    def get_or_create_account(
        self, session: Session, domain: str, client_id: str, client_secret: str
    ) -> Account:
        """
        Get an existing account or create a new one.

        Args:
            session: Database session (provided by decorator)
            domain: Bank domain
            client_id: Client ID for the bank API
            client_secret: Client secret for the bank API

        Returns:
            Account instance
        """
        # Try to find existing account
        account = (
            session.query(Account).filter_by(domain=domain, client_id=client_id).first()
        )

        if account is None:
            # Create new account
            account = Account(
                domain=domain, client_id=client_id, client_secret=client_secret
            )
            session.add(account)
            session.commit()
            session.refresh(account)
            logger.info(f"Created new account: {account}")
        else:
            # Update client_secret if different
            if account.client_secret != client_secret:
                account.client_secret = client_secret
                session.commit()
                logger.info(f"Updated client_secret for account: {account}")

        return account

    @with_session
    def save_authentication(
        self,
        session: Session,
        domain: str,
        client_id: str,
        client_secret: str,
        auth_token: str,
    ) -> Authentication:
        """
        Save authentication token for an account. Search for existing account
        or create a new one if none exists.

        Args:
            session: Database session (provided by decorator)
            domain: Bank domain
            client_id: Client ID for the bank API
            client_secret: Client secret for the bank API
            auth_token: Authentication token

        Returns:
            Authentication instance
        """
        # Get or create account using the existing method
        account = self.get_or_create_account(domain, client_id, client_secret)

        # Check the auth token does not already exist
        existing_auth = (
            session.query(Authentication).filter_by(auth_token=auth_token).first()
        )
        if existing_auth is not None:
            logger.info(
                "Authentication token already exists for account_id="
                f"{existing_auth.account_id}"
            )
            return existing_auth

        # Create new authentication
        auth = Authentication(account_id=account.id, auth_token=auth_token)
        session.add(auth)
        session.commit()
        session.refresh(auth)
        logger.info(
            f"Saved authentication for account_id={account.id} "
            f"(domain={domain}, client_id={client_id})"
        )
        return auth

    @with_session
    def get_authentication(
        self, session: Session, account_id: int
    ) -> Optional[Authentication]:
        """
        Get the most recent authentication for an account.

        Args:
            session: Database session (provided by decorator)
            account_id: Account ID

        Returns:
            Authentication instance or None
        """
        auth = (
            session.query(Authentication)
            .filter_by(account_id=account_id)
            .order_by(Authentication.register_date.desc())
            .first()
        )
        return auth

    @with_session
    def get_authentication_by_credentials(
        self, session: Session, domain: str, client_id: str, client_secret: str
    ) -> Optional[Authentication]:
        """
        Get the most recent authentication for an account by domain,
        client_id, and client_secret.

        Args:
            session: Database session (provided by decorator)
            domain: Bank domain
            client_id: Client ID for the bank API
            client_secret: Client secret for the bank API

        Returns:
            Authentication instance or None
        """
        # First find the account with the given credentials
        account = (
            session.query(Account)
            .filter_by(domain=domain, client_id=client_id, client_secret=client_secret)
            .first()
        )

        if account is None:
            return None

        # Then get the most recent authentication for this account
        auth = (
            session.query(Authentication)
            .filter_by(account_id=account.id)
            .order_by(Authentication.register_date.desc())
            .first()
        )
        return auth

    @with_session
    def get_transactions(
        self,
        session: Session,
        account_id: Optional[int] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Transaction]:
        """
        Get transactions, optionally filtered by account.

        Args:
            session: Database session (provided by decorator)
            account_id: Filter by account ID (optional)
            limit: Maximum number of transactions to return
            offset: Number of transactions to skip

        Returns:
            List of Transaction instances
        """
        query = session.query(Transaction)

        if account_id is not None:
            query = query.filter_by(account_id=account_id)

        query = query.order_by(Transaction.transaction_date.desc())

        if offset > 0:
            query = query.offset(offset)

        if limit is not None:
            query = query.limit(limit)

        return query.all()

    @with_session
    def update_transactions(
        self, session: Session, account_id: int, transactions: List[dict]
    ) -> None:
        """
        Update transactions for a given account. Create new transactions if not existing.

        Args:
            session: Database session (provided by decorator)
            account_id: Account ID to update transactions for
            transactions: List of Transaction instances to update

        Returns:
            None
        """
        # Update existing transactions, add new ones
        for transaction_data in transactions:
            transaction_id = transaction_data.get("id")
            if not transaction_id:
                logger.warning(f"Transaction missing ID, skipping: {transaction_data}")
                continue

            # Check if transaction already exists
            existing_transaction = (
                session.query(Transaction).filter_by(id=transaction_id).first()
            )

            if existing_transaction:
                # Update existing transaction
                existing_transaction.transaction_date = datetime.fromisoformat(
                    transaction_data.get("date", datetime.utcnow().isoformat())
                )
                if (
                    "powens_date" in transaction_data
                    and transaction_data["powens_date"]
                ):
                    existing_transaction.powens_date = datetime.fromisoformat(
                        transaction_data["powens_date"]
                    )
                existing_transaction.data = transaction_data
                logger.debug(f"Updated existing transaction: {transaction_id}")
            else:
                # Create new transaction
                transaction_date = datetime.fromisoformat(
                    transaction_data.get("date", datetime.utcnow().isoformat())
                )
                powens_date = None
                if (
                    "powens_date" in transaction_data
                    and transaction_data["powens_date"]
                ):
                    powens_date = datetime.fromisoformat(
                        transaction_data["powens_date"]
                    )

                new_transaction = Transaction(
                    id=transaction_id,
                    account_id=account_id,
                    transaction_date=transaction_date,
                    powens_date=powens_date,
                    data=transaction_data,
                )
                session.add(new_transaction)
                logger.debug(f"Created new transaction: {transaction_id}")

        session.commit()
        logger.info(
            f"Updated {len(transactions)} transactions for account_id={account_id}"
        )

    @with_session
    def get_accounts(self, session: Session) -> List[Account]:
        """
        Get all accounts.

        Args:
            session: Database session (provided by decorator)

        Returns:
            List of Account instances
        """
        return session.query(Account).all()

    @with_session
    def get_account(
        self, session: Session, client_id: str, domain: str, client_secret: str
    ) -> Optional[Account]:
        """
        Get an account by domain, client_id, and client_secret.

        Args:
            session: Database session (provided by decorator)
            domain: Bank domain
            client_id: Client ID for the bank API
            client_secret: Client secret for the bank API

        Returns:
            Account instance or None
        """
        return (
            session.query(Account)
            .filter_by(domain=domain, client_id=client_id, client_secret=client_secret)
            .first()
        )

    @with_session
    def delete_account(self, session: Session, account_id: int) -> bool:
        """
        Delete an account and all related data.

        Args:
            session: Database session (provided by decorator)
            account_id: Account ID to delete

        Returns:
            True if account was deleted, False if not found
        """
        account = session.query(Account).filter_by(id=account_id).first()
        if account:
            session.delete(account)
            session.commit()
            logger.info(f"Deleted account: {account}")
            return True
        return False

    @with_session
    def latest_sent_transaction(
        self, session: Session, account_id: int
    ) -> Optional[Transaction]:
        """
        Get the most recent sent transaction for an account.

        Args:
            session: Database session (provided by decorator)
            account_id: Account ID

        Returns:
            Transaction instance or None
        """
        transaction = (
            session.query(Transaction)
            .filter_by(account_id=account_id, sent=True)
            .order_by(Transaction.transaction_date.desc())
            .first()
        )
        return transaction


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager(database_url: Optional[str] = None) -> DatabaseManager:
    """
    Get the global database manager instance.

    Args:
        database_url: Database URL (only used on first call)

    Returns:
        DatabaseManager instance
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(database_url)
    return _db_manager


def init_database(database_url: Optional[str] = None) -> DatabaseManager:
    """
    Initialize the database manager.

    Args:
        database_url: SQLAlchemy database URL

    Returns:
        DatabaseManager instance
    """
    global _db_manager
    _db_manager = DatabaseManager(database_url)
    return _db_manager
