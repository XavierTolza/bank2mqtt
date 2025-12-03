# SQLAlchemy database driver for bank2mqtt
# Models: Clients, Authentication, Accounts, Transactions
# Features: context manager, CRUD methods, documentation

from sqlite3 import DatabaseError
from typing import Dict, Optional, Tuple
from venv import logger
from sqlalchemy import (
    BinaryExpression,
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    Boolean,
    ForeignKey,
    DECIMAL,
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from contextlib import contextmanager
from datetime import datetime as dt
import os
from sqlalchemy import func


# --- Models ---


class _Base:
    def to_dict(self):
        """Convert SQLAlchemy model instance to dictionary."""
        return {
            c.name: getattr(self, c.name)
            for c in self.__table__.columns  # type: ignore
        }


Base = declarative_base(cls=_Base)


class Domain(Base):
    """
    Represents a client application.
    id: Primary key (string)
    domain: Client domain
    redirect_uri: Client redirect URL
    created_at: Creation datetime
    """

    __tablename__ = "domain"
    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String, nullable=False, unique=True)
    redirect_uri = Column(String, nullable=True)
    created_at = Column(DateTime, default=dt.now)
    authentications = relationship("Authentication", back_populates="domain")


class Authentication(Base):
    """
    Authentication credentials for a client.
    id: Primary key (int)
    client_id: Foreign key to Client
    client_secret: Secret
    auth_token: Unique access token
    token_creation_date: Datetime
    type: Token type ('temporary'/'permanent')
    id_user: User ID
    expires_in: Expiration in seconds (nullable)
    """

    __tablename__ = "authentications"
    id = Column(Integer, primary_key=True)
    domain_id = Column(Integer, ForeignKey("domain.id"))
    client_id = Column(String, nullable=False)
    client_secret = Column(String, nullable=False)
    auth_token = Column(String, unique=True, nullable=False)
    token_creation_date = Column(DateTime, default=dt.now)
    domain = relationship("Domain", back_populates="authentications")


class Account(Base):
    """
    Bank account details.
    id: Account ID (int)
    id_connection, id_user, id_source, id_parent: Foreign keys (nullable)
    number: Account number (nullable)
    original_name: Original name
    balance, coming: Decimal (nullable)
    display: Show account (bool)
    last_update, deleted, disabled: Datetime (nullable)
    iban: IBAN (nullable)
    currency: Currency (string, nullable)
    type: Account type (string)
    id_type: Account type ID
    bookmarked: Bookmarked (int)
    name: Account name
    error: Error code (nullable)
    usage: Usage (string)
    ownership: Ownership (string, nullable)
    company_name: Company name (nullable)
    loan: Loan details (string, nullable)
    """

    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    id_connection = Column(Integer, nullable=True)
    id_user = Column(Integer, nullable=True)
    id_source = Column(Integer, nullable=True)
    id_parent = Column(Integer, nullable=True)
    number = Column(String, nullable=True)
    original_name = Column(String, nullable=False)
    balance = Column(DECIMAL, nullable=True)
    coming = Column(DECIMAL, nullable=True)
    display = Column(Boolean, default=True)
    # last_update = Column(DateTime, nullable=True)
    deleted = Column(DateTime, nullable=True)
    disabled = Column(DateTime, nullable=True)
    iban = Column(String, nullable=True)
    # currency = Column(String, nullable=True)
    type = Column(String, nullable=False)
    id_type = Column(Integer, nullable=False)
    bookmarked = Column(Integer, default=0)
    name = Column(String, nullable=False)
    error = Column(String, nullable=True)
    usage = Column(String, nullable=False)
    ownership = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    loan = Column(String, nullable=True)
    transactions = relationship("Transaction", back_populates="account")


class Transaction(Base):
    """
    Bank account transaction.
    id: Transaction ID (int)
    id_account: Foreign key to Account
    application_date, date, vdate, rdate, bdate: Date (nullable)
    datetime, vdatetime, rdatetime, bdatetime: DateTime (nullable)
    value, gross_value: Decimal (nullable)
    type: Transaction type (string)
    original_wording: Full label
    simplified_wording: Simplified label
    wording: Editable label (nullable)
    categories: String (JSON, nullable)
    date_scraped: DateTime
    coming: Boolean
    active: Boolean
    id_cluster: Integer (nullable)
    comment: String (nullable)
    last_update: DateTime (nullable)
    deleted: DateTime (nullable)
    original_value, original_gross_value: Decimal (nullable)
    original_currency: String (nullable)
    commission: Decimal (nullable)
    commission_currency: String (nullable)
    country: String (nullable)
    card: String (nullable)
    counterparty: String (nullable)
    """

    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    id_account = Column(Integer, ForeignKey("accounts.id"))
    application_date = Column(String, nullable=True)
    date = Column(String, nullable=False)
    vdate = Column(String, nullable=True)
    rdate = Column(String, nullable=False)
    bdate = Column(String, nullable=True)
    value = Column(DECIMAL, nullable=True)
    type = Column(String, nullable=False)
    original_wording = Column(String, nullable=False)
    simplified_wording = Column(String, nullable=False)
    wording = Column(String, nullable=True)
    categories = Column(String, nullable=True)  # JSON string
    date_scraped = Column(String, nullable=False)
    coming = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    id_cluster = Column(Integer, nullable=True)
    comment = Column(String, nullable=True)
    last_update = Column(String, nullable=True)
    deleted = Column(String, nullable=True)
    original_value = Column(DECIMAL, nullable=True)
    original_gross_value = Column(DECIMAL, nullable=True)
    country = Column(String, nullable=True)
    card = Column(String, nullable=True)
    account = relationship("Account", back_populates="transactions")


class AccountBalance(Base):
    """
    Store account balance snapshot.
    Fields:
      - account_id: FK to accounts.id (unique, one-to-one)
      - balance: current balance
      - coming_balance: pending/coming balance
      - last_update: datetime of last update
    """

    __tablename__ = "account_balances"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, unique=True)
    balance = Column(DECIMAL, nullable=False)
    coming_balance = Column(DECIMAL, nullable=False)
    last_update = Column(DateTime, nullable=False)


# --- Database Driver ---


class Bank2MQTTDatabase:
    """
    SQLAlchemy database driver for bank2mqtt.
    Usage:
            with Bank2MQTTDatabase(db_url) as db:
                    ...
    """

    @classmethod
    def from_env(cls):
        """
        Create a Bank2MQTTDatabase instance using environment variables.
        Uses BANK2MQTT_DB_URL or DATABASE_URL.
        """

        db_url = os.getenv("BANK2MQTT_DB_URL") or os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError(
                "Database URL not found in environment variables (BANK2MQTT_DB_URL "
                "or DATABASE_URL)"
            )
        return cls(db_url)

    def __init__(self, url):
        logger.info(f"Opening database at {url}")
        try:
            self.engine = create_engine(url)
            self.Session = sessionmaker(bind=self.engine)
            Base.metadata.create_all(self.engine)
        except DatabaseError as e:
            raise RuntimeError(f"Failed to open Database {url}: {e}") from e

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # --- Methods ---

    def get_domain_and_latest_auth(
        self, domain, client_id
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        Get domain and latest authentication by domain and client_id.
        Returns (Domain, Authentication) or (None, None)
        """
        with self.session_scope() as session:
            domain = session.query(Domain).filter_by(domain=domain).first()
            if not domain:
                return None, None
            auth = (
                session.query(Authentication)
                .filter_by(domain_id=domain.id)
                .order_by(Authentication.token_creation_date.desc())
                .first()
            )
            return domain.to_dict(), auth.to_dict() if auth else None

    def register_domain_and_auth(self, domain_data, auth_data):
        """
        Register a new domain and authentication.
        domain_data: dict for Domain
        auth_data: dict for Authentication
        Returns (Domain, Authentication)
        """
        with self.session_scope() as session:
            # Insert domain
            # Check if domain already exists
            if (
                domain := session.query(Domain)
                .filter_by(domain=domain_data["domain"])
                .first()
            ):
                # Domain already exists
                domain_id = domain.id
            else:
                domain = Domain(**domain_data)
                session.add(domain)
                session.flush()
                domain_id = domain.id

            # Insert authentication
            # Check if auth with same token already exists
            if (
                auth := session.query(Authentication)
                .filter_by(auth_token=auth_data["auth_token"])
                .first()
            ):
                return domain, auth  # Auth already exists
            else:
                # Insert new auth
                auth = Authentication(**auth_data, domain_id=domain_id)
                session.add(auth)
                session.commit()
            return domain, auth

    def filter_transactions(self, order=None, **filters):
        """
        Filter and retrieve transactions by filters (kwargs).
        order: SQLAlchemy order expression or column name string
        Returns list of Transaction
        """
        with self.session_scope() as session:
            query = session.query(Transaction)
            for key, value in filters.items():
                if not isinstance(value, BinaryExpression):
                    value = getattr(Transaction, key) == value
                query = query.filter(value)

            if order is not None:
                if isinstance(order, str):
                    order = getattr(Transaction, order)
                query = query.order_by(order)

            res = [i.to_dict() for i in query.all()]
            return res

    def get_latest_transactions(self, auth_id, limit=10):
        """
        Get latest transactions for a given authentication.
        Returns list of Transaction
        """
        with self.session_scope() as session:
            # Find accounts for this auth's user
            auth = session.query(Authentication).filter_by(id=auth_id).first()
            if not auth:
                return []
            accounts = session.query(Account).filter_by(id_user=auth.id_user).all()
            account_ids = [a.id for a in accounts]
            txs = (
                session.query(Transaction)
                .filter(Transaction.id_account.in_(account_ids))
                .order_by(Transaction.date.desc())
                .limit(limit)
                .all()
            )
            return txs

    def latest_transaction_date(
        self, account_id: Optional[int] = None
    ) -> Optional[str]:
        """
        Get the latest transaction date, optionally filtered by account.
        Returns datetime or None
        """
        with self.session_scope() as session:
            query = session.query(Transaction).order_by(Transaction.date.desc())
            if account_id is not None:
                query = query.filter(Transaction.id_account == account_id)
            tx = query.first()
            return str(tx.date) if tx else None

    def upsert_transactions(self, transactions):
        """
        Register new transactions from a list.
        If already recorded (by id), update; else create.
        transactions: list of dict
        Returns list of Transaction
        """
        with self.session_scope() as session:
            results = []
            for tx_data in transactions:
                tx = session.query(Transaction).filter_by(id=tx_data["id"]).first()
                if tx:
                    for k, v in tx_data.items():
                        setattr(tx, k, v)
                else:
                    tx = Transaction(
                        **{
                            k: v
                            for k, v in tx_data.items()
                            if k in Transaction.__table__.columns.keys()
                        }
                    )
                    session.add(tx)
                results.append(tx)
            session.commit()
            return results

    def upsert_accounts(self, accounts):
        """
        Register new accounts from a list.
        If already recorded (by id), update; else create.
        accounts: list of dict
        Returns list of Account
        """
        with self.session_scope() as session:
            results = []
            for acc_data in accounts:
                acc = session.query(Account).filter_by(id=acc_data["id"]).first()
                if acc:
                    for k, v in acc_data.items():
                        setattr(acc, k, v)
                else:
                    acc = Account(
                        **{
                            k: v
                            for k, v in acc_data.items()
                            if k in Account.__table__.columns.keys()
                        }
                    )
                    session.add(acc)
                results.append(acc)
            session.commit()
            return results

    # New methods: get_account_balance, upsert_account_balance, upsert_account_balances
    def get_account_balance(self, account_id):
        """
        Return the AccountBalance row for account_id or None.
        """
        with self.session_scope() as session:
            ab = session.query(AccountBalance).filter_by(account_id=account_id).first()
            return ab

    def upsert_account_balance(self, account_id, balance, coming_balance, last_update):
        """
        Insert or update a single AccountBalance.
        If an existing record for account_id has the same last_update, no change is made.
        last_update may be a datetime, ISO string, or numeric timestamp.
        Returns the AccountBalance instance (existing or new).
        """
        # normalize last_update to datetime
        if isinstance(last_update, str):
            try:
                last_dt = dt.fromisoformat(last_update)
            except Exception:
                # fallback: try to parse as numeric timestamp
                try:
                    last_dt = dt.fromtimestamp(float(last_update))
                except Exception:
                    raise ValueError(
                        "last_update string is not ISO format or timestamp"
                    )
        elif isinstance(last_update, (int, float)):
            last_dt = dt.fromtimestamp(last_update)
        elif isinstance(last_update, dt):
            last_dt = last_update
        else:
            raise ValueError("Unsupported last_update type")

        with self.session_scope() as session:
            existing = (
                session.query(AccountBalance).filter_by(account_id=account_id).first()
            )
            if existing and existing.last_update == last_dt:
                # Record already exists with same timestamp, no update needed
                return existing

            # Create new AccountBalance record
            ab = AccountBalance(
                account_id=account_id,
                balance=balance,
                coming_balance=coming_balance,
                last_update=last_dt,
            )
            session.add(ab)

            session.commit()
            return ab

    def upsert_account_balances(self, balances):
        """
        Batch upsert balances.
        balances: iterable of dicts with keys: account_id, balance, coming_balance, last_update
        Returns list of AccountBalance instances.
        """
        results = []
        for b in balances:
            res = self.upsert_account_balance(
                b["id"], b["balance"], b["coming_balance"], b["last_update"]
            )
            results.append(res)
        return results

    def last_account_balance(self):
        """
        Get the most recent account balance snapshot.
        Returns AccountBalance or None
        """
        with self.session_scope() as session:
            # Return the most recent AccountBalance row for each account_id.
            # Use a grouped subquery to get max(last_update) per account, then join back.

            subq = (
                session.query(
                    AccountBalance.account_id,
                    func.max(AccountBalance.last_update).label("max_update"),
                )
                .group_by(AccountBalance.account_id)
                .all()
            )
            res = {k: v for k, v in subq}
            return res
