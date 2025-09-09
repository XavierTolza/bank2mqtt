import os
from typing import Dict
import time
from typing import Any
import click
from bank2mqtt.client import PowensClient as Client
from bank2mqtt.handlers.mqtt import MqttHandler
from dotenv import load_dotenv
import json
from loguru import logger

# Load environment variables
load_dotenv()


@click.group()
def cli():
    """Powens Banking API CLI"""
    logger.info("Starting bank2mqtt CLI application")
    logger.debug("CLI group initialized")


@cli.command()
def get_url():
    """Get the Powens Connect Webview URL"""
    logger.info("Generating Powens Connect Webview URL")
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")
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
@click.option("--account-id", type=int, help="Account ID to filter transactions")
@click.option("--limit", type=int, default=1000, help="Number of transactions to fetch")
@click.option("--date-from", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--date-to", type=str, help="End date (YYYY-MM-DD)")
@click.option("--csv", "csv_file", type=str, help="Save transactions to CSV file")
def list_transactions(account_id, limit, date_from, date_to, csv_file):
    """List transactions for an account or all accounts."""
    logger.info(
        f"Listing transactions (account_id={account_id}, limit={limit}, "
        f"date_from={date_from}, date_to={date_to}, csv_file={csv_file})"
    )
    try:
        client = Client.from_env()
        logger.debug("Client created from environment variables")

        txs = client.list_transactions(
            account_id=account_id, limit=limit, date_from=date_from, date_to=date_to
        )

        transaction_count = len(txs)
        logger.success(f"Retrieved {transaction_count} transactions")

        # Log transaction summary
        if transaction_count > 0:
            from_date = date_from or "unlimited"
            to_date = date_to or "unlimited"
            logger.debug(f"Date range in results: {from_date} to {to_date}")

        if csv_file:
            import pandas as pd

            # Save transactions to CSV file using pandas
            logger.info(
                f"Saving {transaction_count} transactions to CSV file: {csv_file}"
            )
            try:
                # Convert transactions to DataFrame
                df = pd.json_normalize(txs)

                # Save to CSV
                df.to_csv(csv_file, index=False, encoding="utf-8")

                logger.success(f"Transactions successfully saved to: {csv_file}")
                click.echo(f"Transactions saved to: {csv_file}")
            except Exception as csv_error:
                logger.error(f"Failed to save CSV file: {csv_error}")
                click.echo(f"Error saving CSV file: {csv_error}", err=True)
        else:
            click.echo(json.dumps(txs, indent=2))
    except Exception as e:
        logger.error(f"Failed to list transactions: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
def run():
    # Get the client
    client = Client.from_env()
    logger.debug("Client created from environment variables")

    sleep_interval = int(os.getenv("SLEEP_INTERVAL", 60 * 60 * 2))

    # Display url to manage accounts
    url = client.get_webview_url()
    logger.info(f"You can manage your accounts at: {url}")

    with MqttHandler.from_env() as mqtt_handler:
        # Retrieve bank accounts
        accounts = client.list_accounts(all_accounts=True)
        logger.info(f"Retrieved {len(accounts)} accounts")
        if len(accounts) == 0:
            logger.warning("No accounts found.")
            click.echo("No accounts found.")
            return
        accounts_by_id = {account["id"]: account for account in accounts}

        params: Dict[str, Any] = {"limit": 1000}
        while True:
            transaction = client.db.latest_sent_transaction(client.db_account_id)
            latest_date = transaction["date"] if transaction else None
            transactions = client.list_transactions(date_from=latest_date)
            raise NotImplementedError
            # Get new transactions
            new_transactions = client.list_transactions(**params)
            if len(new_transactions):
                mqtt_handler.process_transaction(
                    [
                        {**tx, "account": accounts_by_id.get(tx["id_account"])}
                        for tx in new_transactions
                    ]
                )
                start_date = new_transactions[-1]["last_update"]
                client.cache.set("last_date", start_date)
                params["last_update"] = start_date
                logger.debug(
                    f"Updated last transaction date to: {client.cache.get('last_date')}"
                )
            else:
                logger.debug(
                    (
                        f"Sleeping for {sleep_interval} seconds before next fetch. "
                        f"You can manage your accounts at: {url}"
                    )
                )
                time.sleep(sleep_interval)
    client.cache.close()


if __name__ == "__main__":
    logger.debug("Application starting from main entry point")
    # Add the stream command to the CLI group
    cli.add_command(run)
    try:
        cli()
    except Exception as e:
        logger.critical(f"Critical error in main application: {e}")
        raise
    finally:
        logger.debug("Application shutdown")
