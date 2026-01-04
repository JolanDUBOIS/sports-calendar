import typer 

from .utils import with_config
from src.config import Config
from src.app import run_selection as run_selection_logic, clear_calendar


calendar_app = typer.Typer(help="Commands to run and manage the calendar selection and utils.")

@calendar_app.command()
@with_config
def run_selection(
    ctx: typer.Context,
    key: str = typer.Argument("dev"),
    repo: str | None = typer.Option(None, "--repo", help="Specify the repository to run the selection on (default in infrastructure.yml config file)."),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Run the data selection. """
    run_selection_logic(
        config=ctx.obj.get("config"),
        key=key,
        dry_run=dry_run,
        verbose=verbose
    )

@calendar_app.command()
def clear(
    key: str = typer.Argument("dev"),
    scope: str | None = typer.Option(None, "--scope", help="Specify which events to clear: 'all', 'future', or 'past'."),
    date_from: str | None = typer.Option(None, "--date-from", help="Clear events from this date onwards (YYYY-MM-DD). Not needed if --scope is specified."),
    date_to: str | None = typer.Option(None, "--date-to", help="Clear events up to this date (YYYY-MM-DD). Not needed if --scope is specified."),
    verbose: bool = typer.Option(False, "--verbose")
):
    """ Clear events from the Google Calendar. """
    if scope is not None and scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)
    
    clear_calendar(
        key=key,
        scope=scope,
        date_from=date_from,
        date_to=date_to,
        verbose=verbose
    )
