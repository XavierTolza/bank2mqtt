import sys
from time import time
import click
from bank2mqtt.client import PowensClient as Client
from bank2mqtt.logging import get_logger, setup_logging
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Setup logging as early as possible
setup_logging()
logger = get_logger(__name__)


@click.group()
def cli():
    """Powens Banking API CLI"""
    logger.info("Starting bank2mqtt CLI application")
    logger.debug("CLI group initialized")


@cli.command()
def authenticate():
    """Authenticate and get a permanent auth token."""
    logger.info("Starting authentication process")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        token = client.authenticate()
        logger.success("Authentication successful")
        logger.debug(f"Token length: {len(token) if token else 0} characters")
        click.echo(token)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
def get_temp_code():
    """Get a one-time temporary code."""
    logger.info("Requesting temporary code")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        client.authenticate()
        logger.debug("Authentication completed")
        code = client.get_temp_code()
        logger.success("Temporary code generated successfully")
        logger.debug(f"Code length: {len(code) if code else 0} characters")
        click.echo(code)
    except Exception as e:
        logger.error(f"Failed to get temporary code: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
def get_connect_url():
    """Get the Powens Connect Webview URL"""
    logger.info("Generating Powens Connect Webview URL")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        client.authenticate()
        logger.debug("Authentication completed")
        url = client.get_webview_url()
        logger.success("Webview URL generated successfully")
        logger.debug(f"Generated URL: {url}")
        click.echo(url)
    except Exception as e:
        logger.error(f"Failed to get connect URL: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option("--all", "all_accounts", is_flag=True, help="Include disabled accounts")
def list_accounts(all_accounts):
    """List user bank accounts."""
    logger.info(f"Listing accounts (include_disabled={all_accounts})")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        client.authenticate()
        logger.debug("Authentication completed")
        accounts = client.list_accounts(all_accounts=all_accounts)
        logger.success(f"Retrieved {len(accounts)} accounts")

        # Log account summary
        for account in accounts:
            account_id = account.get("id", "unknown")
            account_name = account.get("name", "unknown")
            account_type = account.get("type", "unknown")
            disabled = account.get("disabled", False)
            status = "disabled" if disabled else "active"
            logger.debug(
                f"Account {account_id}: {account_name} ({account_type}) - {status}"
            )

        click.echo(json.dumps(accounts, indent=2))
    except Exception as e:
        logger.error(f"Failed to list accounts: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("account_id", type=int)
def activate_account(account_id):
    """Activate a disabled account by ID."""
    logger.info(f"Activating account {account_id}")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        client.authenticate()
        logger.debug("Authentication completed")
        result = client.activate_account(account_id)
        logger.success(f"Account {account_id} activated successfully")
        logger.debug(f"Activation result: {result}")
        click.echo(json.dumps(result, indent=2))
    except Exception as e:
        logger.error(f"Failed to activate account {account_id}: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option("--account-id", type=int, help="Account ID to filter transactions")
@click.option("--limit", type=int, default=50, help="Number of transactions to fetch")
@click.option("--date-from", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--date-to", type=str, help="End date (YYYY-MM-DD)")
def list_transactions(account_id, limit, date_from, date_to):
    """List transactions for an account or all accounts."""
    logger.info(
        f"Listing transactions (account_id={account_id}, limit={limit}, "
        f"date_from={date_from}, date_to={date_to})"
    )
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
        client.authenticate()
        logger.debug("Authentication completed")

        txs = client.list_transactions(
            account_id=account_id, limit=limit, date_from=date_from, date_to=date_to
        )

        transaction_count = len(txs.get("transactions", []))
        logger.success(f"Retrieved {transaction_count} transactions")

        # Log transaction summary
        if transaction_count > 0:
            from_date = date_from or "unlimited"
            to_date = date_to or "unlimited"
            logger.debug(f"Date range in results: {from_date} to {to_date}")

        click.echo(json.dumps(txs, indent=2))
    except Exception as e:
        logger.error(f"Failed to list transactions: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@click.command()
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Chemin vers le fichier de configuration YAML.",
)
@click.option(
    "--loop", is_flag=True, help="Activer le mode boucle pour streamer en continu."
)
def stream(config_path: str, loop: bool):
    """Récupère les transactions bancaires et les envoie aux handlers configurés."""
    config = load_config(config_path)

    client_config = config.get("bank_client", {})
    if not all(k in client_config for k in ["domain", "client_id", "client_secret"]):
        print(
            "Erreur: La configuration 'bank_client' est incomplète dans le fichier YAML."
        )
        sys.exit(1)

    # Initialiser le client bancaire (à adapter selon votre implémentation)
    # bank_client = BankClient(
    #     domain=client_config['domain'],
    #     client_id=client_config['client_id'],
    #     client_secret=client_config['client_secret']
    # )

    handlers = setup_handlers(config)
    if not handlers:
        print("Aucun handler actif. Le programme va s'arrêter.")
        sys.exit(0)

    interval = config.get("loop_interval", 86400)  # 24h par défaut

    def run_once():
        print("Récupération des nouvelles transactions...")
        # --- LOGIQUE DE RÉCUPÉRATION ---
        # new_transactions, account_info = bank_client.get_new_transactions()
        # C'est ici que vous mettriez votre logique pour appeler le client.
        # Pour la démo, nous utilisons des données factices :
        new_transactions = [
            {
                "date": "2025-08-26",
                "description": "Exemple de transaction",
                "amount": -99.99,
            }
        ]
        account_info = {"name": "Compte courant", "currency": "EUR"}
        # --------------------------------

        if not new_transactions:
            print("Aucune nouvelle transaction trouvée.")
            return

        print(f"{len(new_transactions)} nouvelle(s) transaction(s) trouvée(s).")
        for transaction in new_transactions:
            for handler in handlers:
                try:
                    handler.process_transaction(
                        {"transaction": transaction, "account": account_info}
                    )
                except Exception as e:
                    print(f"Erreur lors du traitement par un handler : {e}")

    # Exécution principale
    try:
        if loop:
            print(
                f"Mode boucle activé. Le cycle se répétera toutes les {interval} secondes."
            )
            while True:
                run_once()
                print(
                    f"Attente de {interval} secondes avant le prochain cycle... (Ctrl+C pour arrêter)"
                )
                time.sleep(interval)
        else:
            print("Exécution unique.")
            run_once()
    except KeyboardInterrupt:
        print("\nProgramme interrompu par l'utilisateur. Arrêt en cours...")
        sys.exit(0)


if __name__ == "__main__":
    logger.debug("Application starting from main entry point")
    try:
        cli()
    except Exception as e:
        logger.critical(f"Critical error in main application: {e}")
        raise
    finally:
        logger.debug("Application shutdown")
