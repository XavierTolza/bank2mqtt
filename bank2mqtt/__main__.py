import click
from bank2mqtt.client import PowensClient as Client
from dotenv import load_dotenv
import json

load_dotenv()


@click.group()
def cli():
    """Powens Banking API CLI"""
    pass


@cli.command()
def authenticate():
    """Authenticate and get a permanent auth token."""
    client = Client.from_env()
    token = client.authenticate()
    click.echo(token)


@cli.command()
def get_temp_code():
    """Get a one-time temporary code."""
    client = Client.from_env()
    client.authenticate()
    code = client.get_temp_code()
    click.echo(code)


@cli.command()
@click.argument("code")
def get_connect_url(code):
    """Get the Powens Connect Webview URL for a code."""
    client = Client.from_env()
    url = client.get_connect_url(code)
    click.echo(url)


@cli.command()
@click.option("--all", "all_accounts", is_flag=True, help="Include disabled accounts")
def list_accounts(all_accounts):
    """List user bank accounts."""
    client = Client.from_env()
    client.authenticate()
    accounts = client.list_accounts(all_accounts=all_accounts)
    click.echo(json.dumps(accounts, indent=2))


@cli.command()
@click.argument("account_id", type=int)
def activate_account(account_id):
    """Activate a disabled account by ID."""
    client = Client.from_env()
    client.authenticate()
    result = client.activate_account(account_id)
    click.echo(json.dumps(result, indent=2))


@cli.command()
@click.option("--account-id", type=int, help="Account ID to filter transactions")
@click.option("--limit", type=int, default=50, help="Number of transactions to fetch")
@click.option("--date-from", type=str, help="Start date (YYYY-MM-DD)")
@click.option("--date-to", type=str, help="End date (YYYY-MM-DD)")
def list_transactions(account_id, limit, date_from, date_to):
    """List transactions for an account or all accounts."""
    client = Client.from_env()
    client.authenticate()
    txs = client.list_transactions(
        account_id=account_id, limit=limit, date_from=date_from, date_to=date_to
    )
    click.echo(json.dumps(txs, indent=2))


if __name__ == "__main__":
    cli()
