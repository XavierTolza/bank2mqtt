# SQLAlchemy database driver for bank2mqtt
# Models: Clients, Authentication, Accounts, Transactions
# Features: context manager, CRUD methods, documentation

from sqlalchemy import (
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
from datetime import datetime

Base = declarative_base()

# --- Models ---


class Client(Base):
    """
    Represents a client application.
    id: Primary key (string)
    domain: Client domain
    redirect_url: Client redirect URL
    created_at: Creation datetime
    """

    __tablename__ = "clients"
    id = Column(String, primary_key=True)
    domain = Column(String, nullable=False)
    redirect_url = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    authentications = relationship("Authentication", back_populates="client")


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
    client_id = Column(String, ForeignKey("clients.id"))
    client_secret = Column(String, nullable=False)
    auth_token = Column(String, unique=True, nullable=False)
    token_creation_date = Column(DateTime, default=datetime.utcnow)
    type = Column(String, nullable=False)
    id_user = Column(Integer, nullable=False)
    expires_in = Column(Integer, nullable=True)
    client = relationship("Client", back_populates="authentications")


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
    last_update = Column(DateTime, nullable=True)
    deleted = Column(DateTime, nullable=True)
    disabled = Column(DateTime, nullable=True)
    iban = Column(String, nullable=True)
    currency = Column(String, nullable=True)
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
    application_date = Column(DateTime, nullable=True)
    date = Column(DateTime, nullable=False)
    datetime = Column(DateTime, nullable=True)
    vdate = Column(DateTime, nullable=True)
    vdatetime = Column(DateTime, nullable=True)
    rdate = Column(DateTime, nullable=False)
    rdatetime = Column(DateTime, nullable=True)
    bdate = Column(DateTime, nullable=True)
    bdatetime = Column(DateTime, nullable=True)
    value = Column(DECIMAL, nullable=True)
    gross_value = Column(DECIMAL, nullable=True)
    type = Column(String, nullable=False)
    original_wording = Column(String, nullable=False)
    simplified_wording = Column(String, nullable=False)
    wording = Column(String, nullable=True)
    categories = Column(String, nullable=True)  # JSON string
    date_scraped = Column(DateTime, default=datetime.utcnow)
    coming = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    id_cluster = Column(Integer, nullable=True)
    comment = Column(String, nullable=True)
    last_update = Column(DateTime, nullable=True)
    deleted = Column(DateTime, nullable=True)
    original_value = Column(DECIMAL, nullable=True)
    original_gross_value = Column(DECIMAL, nullable=True)
    original_currency = Column(String, nullable=True)
    commission = Column(DECIMAL, nullable=True)
    commission_currency = Column(String, nullable=True)
    country = Column(String, nullable=True)
    card = Column(String, nullable=True)
    counterparty = Column(String, nullable=True)
    account = relationship("Account", back_populates="transactions")


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
        import os

        db_url = os.getenv("BANK2MQTT_DB_URL") or os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError(
                "Database URL not found in environment variables (BANK2MQTT_DB_URL or DATABASE_URL)"
            )
        return cls(db_url)

    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

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

    def get_client_and_latest_auth(self, domain, client_id):
        """
        Get client and latest authentication by domain and client_id.
        Returns (Client, Authentication) or (None, None)
        """
        with self.session_scope() as session:
            client = (
                session.query(Client).filter_by(domain=domain, id=client_id).first()
            )
            if not client:
                return None, None
            auth = (
                session.query(Authentication)
                .filter_by(client_id=client_id)
                .order_by(Authentication.token_creation_date.desc())
                .first()
            )
            return client, auth

    def register_client_and_auth(self, client_data, auth_data):
        """
        Register a new client and authentication.
        client_data: dict for Client
        auth_data: dict for Authentication
        Returns (Client, Authentication)
        """
        with self.session_scope() as session:
            client = Client(**client_data)
            session.add(client)
            session.flush()
            auth_data["client_id"] = client.id
            auth = Authentication(**auth_data)
            session.add(auth)
            session.commit()
            return client, auth

    def filter_transactions(self, **filters):
        """
        Filter and retrieve transactions by filters (kwargs).
        Returns list of Transaction
        """
        with self.session_scope() as session:
            query = session.query(Transaction)
            for key, value in filters.items():
                query = query.filter(getattr(Transaction, key) == value)
            return query.all()

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
                    tx = Transaction(**tx_data)
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
                    acc = Account(**acc_data)
                    session.add(acc)
                results.append(acc)
            session.commit()
            return results
