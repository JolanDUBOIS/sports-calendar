import typer

from .extras import missing_extra

sync_calendar = typer.Typer(help="Commands to run and manage the calendar selection and utils.")

@sync_calendar.callback(invoke_without_command=True)
def main_run(
    name: str = typer.Argument("dev"),
    dry_run: bool = typer.Option(False, "--dry-run")
):
    """ Run the data selection. """
    try:
        from sports_calendar.application.workflows.run_selection import run_selection
    except ImportError as exc:
        raise missing_extra("backend", exc) from exc

    run_selection(
        name=name,
        dry_run=dry_run
    )


clear_cal = typer.Typer(help="Commands to clear events from the Google Calendar.")

@clear_cal.callback(invoke_without_command=True)
def main_clear(
    name: str = typer.Argument("dev"),
    scope: str | None = typer.Option(None, "--scope", help="Specify which events to clear: 'all', 'future', or 'past'."),
    date_from: str | None = typer.Option(None, "--date-from", help="Clear events from this date onwards (YYYY-MM-DD). Not needed if --scope is specified."),
    date_to: str | None = typer.Option(None, "--date-to", help="Clear events up to this date (YYYY-MM-DD). Not needed if --scope is specified.")
):
    """ Clear events from the Google Calendar. """
    if scope is not None and scope not in ["all", "future", "past"]:
        typer.echo("Error: Invalid value for --scope. Valid options are 'all', 'future', or 'past'.", err=True)
        raise typer.Exit(code=1)

    try:
        from sports_calendar.application.workflows.clear_calendar import clear_calendar
    except ImportError as exc:
        raise missing_extra("backend", exc) from exc

    clear_calendar(
        name=name,
        scope=scope,
        date_from=date_from,
        date_to=date_to
    )
