import json
import time
import click
import pandas as pd
from bank2mqtt.config import Config
from loguru import logger

from bank2mqtt.db import Transaction
from datetime import datetime, timedelta

conf = Config.from_env()
db = conf.db
client = conf.client
mqtt = conf.mqtt_handler


@click.group()
def cli():
    """Powens Banking API CLI"""
    logger.info("Starting bank2mqtt CLI application")
    logger.debug("CLI group initialized")


@cli.command()
def get_url():
    """Get the Powens Connect Webview URL"""
    logger.debug("Generating Powens Connect Webview URL")
    try:
        url = client.get_webview_url()
        click.echo("Your Powens Connect Webview URL is:")
        click.echo(url)
    except Exception as e:
        logger.error(f"Failed to get connect URL: {e}")
        click.echo(f"Error: {e}", err=True)
        raise click.Abort()


@cli.command()
def list_accounts():
    """List user bank accounts."""
    try:
        accounts = client.list_accounts()
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
    txs = client.list_transactions(
        account_id=account_id, limit=limit, date_from=date_from, date_to=date_to
    )

    transaction_count = len(txs)
    logger.success(f"Retrieved {transaction_count} transactions")

    # Convert transactions to DataFrame
    df = pd.json_normalize(txs)

    # Log transaction summary
    if transaction_count > 0:
        from_date = date_from or "unlimited"
        to_date = date_to or "unlimited"
        logger.debug(f"Date range in results: {from_date} to {to_date}")

    if csv_file:
        # Save transactions to CSV file using pandas
        logger.info(f"Saving {transaction_count} transactions to CSV file: {csv_file}")
        try:
            # Save to CSV
            df.to_csv(csv_file, index=False, encoding="utf-8")

            logger.success(f"Transactions successfully saved to: {csv_file}")
            click.echo(f"Transactions saved to: {csv_file}")
        except Exception as csv_error:
            logger.error(f"Failed to save CSV file: {csv_error}")
            click.echo(f"Error saving CSV file: {csv_error}", err=True)
    else:
        click.echo(
            df[
                [
                    "id",
                    "id_account",
                    "date",
                    "formatted_value",
                    "simplified_wording",
                ]
            ]
        )


def get_accounts():
    # Retrieve bank accounts
    accounts = client.list_accounts(all_accounts=True)
    logger.info(f"Retrieved {len(accounts)} accounts")
    accounts_by_id = {account["id"]: account for account in accounts}

    # Update the accounts in the database
    db.upsert_accounts(accounts)

    return accounts_by_id


@cli.command()
def run():
    # Display url to manage accounts
    url = client.get_webview_url()
    logger.info(f"You can manage your accounts at: {url}")

    latest_date = db.latest_transaction_date()

    with mqtt:
        while True:
            accounts = get_accounts()
            last_account_balance = db.last_account_balance()

            # Update account balances if changed
            accounts_balance_to_update = [
                acc
                for acc_id, acc in accounts.items()
                if last_account_balance.get(acc_id)
                != datetime.fromisoformat(acc["last_update"])
            ]
            if len(accounts_balance_to_update):
                db.upsert_account_balances(accounts_balance_to_update)

            if len(accounts) == 0:
                logger.warning("No accounts found.")
                click.echo("No accounts found.")
                return

            if latest_date is not None:
                date_from = (
                    datetime.fromisoformat(latest_date) - timedelta(days=3)
                ).isoformat()
                date_filter = dict(
                    date=Transaction.date
                    >= (
                        datetime.fromisoformat(latest_date) - timedelta(days=4)
                    ).isoformat()
                )
            else:
                date_from = None
                date_filter = {}

            transactions = client.list_transactions(limit=1000, date_from=date_from)

            # Find the transactions that are not yet in the database
            transactions_in_db = db.filter_transactions(**date_filter, order="date")
            in_db_ids = {i["id"] for i in transactions_in_db}

            # Add new transactions to the database
            new_transactions = [t for t in transactions if t["id"] not in in_db_ids]
            if len(new_transactions) > 0:
                logger.info(f"Found {len(new_transactions)} new transactions")

                # Publish new transactions to MQTT
                mqtt.process_transaction(
                    [
                        {**tx, "account": accounts.get(tx["id_account"])}
                        for tx in new_transactions
                    ]
                )

                # Update the transactions in the database
                db.upsert_transactions(new_transactions)
                latest_date = max(
                    datetime.fromisoformat(tx["date"]) for tx in new_transactions
                ).isoformat()
            else:
                logger.debug("No new transactions found.")
                logger.info(
                    (
                        f"Sleeping for {conf.sleep_interval} seconds before next fetch. "
                        f"You can manage your accounts at: {url}"
                    )
                )
                time.sleep(conf.sleep_interval)

    client.cache.close()


if __name__ == "__main__":
    logger.debug("Application starting from main entry point")
    # Add the stream command to the CLI group
    try:
        cli()
    except Exception as e:
        logger.critical(f"Critical error in main application: {e}")
        raise
    finally:
        logger.debug("Application shutdown")
