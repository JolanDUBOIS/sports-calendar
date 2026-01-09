import typer 

from .main import run_selection, clear_calendar


sync_calendar = typer.Typer(help="Commands to run and manage the calendar selection and utils.")

@sync_calendar.callback(invoke_without_command=True)
def main(
    key: str = typer.Argument("dev"),
    dry_run: bool = typer.Option(False, "--dry-run")
):
    """ Run the data selection. """
    run_selection(
        key=key,
        dry_run=dry_run
    )


clear_cal = typer.Typer(help="Commands to clear events from the Google Calendar.")

@clear_cal.callback(invoke_without_command=True)
def main(
    key: str = typer.Argument("dev"),
    scope: str | None = typer.Option(None, "--scope", help="Specify which events to clear: 'all', 'future', or 'past'."),
    date_from: str | None = typer.Option(None, "--date-from", help="Clear events from this date onwards (YYYY-MM-DD). Not needed if --scope is specified."),
    date_to: str | None = typer.Option(None, "--date-to", help="Clear events up to this date (YYYY-MM-DD). Not needed if --scope is specified.")
):
    """ Clear events from the Google Calendar. """
    if scope is not None and scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)
    
    clear_calendar(
        key=key,
        scope=scope,
        date_from=date_from,
        date_to=date_to
    )
