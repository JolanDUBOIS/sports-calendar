import typer

from sports_calendar.infra.setup import init_environment

from ... import __version__
from .dev import dev_tools
from .gui import launch_gui
from .sync import clear_cal, sync_calendar

app = typer.Typer(help="Sports Calendar CLI Application — manage DB, calendar, validation.")

app.add_typer(dev_tools, name="devtools", help="Development tools for the sports calendar application.")
app.add_typer(sync_calendar, name="sync-calendar", help="Commands to manage calendar selection.")
app.add_typer(clear_cal, name="clear-calendar", help="Commands to clear events from the Google Calendar.")
app.add_typer(launch_gui, name="launch-gui", help="Launch the Sports Calendar GUI application.")

app.command(name="init")(init_environment)


def version_callback(value: bool):
    if value:
        typer.echo(f"sports-calendar {__version__}")
        raise typer.Exit

@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(False, "--version", "-v", help="Show the version and exit.", is_eager=True, callback=version_callback),
):
    """
    Initialize logging and paths before any subcommand runs, except 'init'.
    """
    if ctx.invoked_subcommand == "init":
        return

    init_environment()
