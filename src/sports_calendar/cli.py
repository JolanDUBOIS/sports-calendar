import typer

from . import logger
from .sync_db import sync_db
from .sync_calendar import sync_calendar, clear_cal
from .validate_db import validate_db
from .sc_core.setup import Paths, setup_logging


app = typer.Typer(help="Sports Calendar CLI Application — manage DB, calendar, validation.")

app.add_typer(sync_db, name="sync-db", help="Commands to manage the data synchronization pipeline.")
app.add_typer(sync_calendar, name="sync-calendar", help="Commands to manage calendar selection.")
app.add_typer(clear_cal, name="clear-calendar", help="Commands to clear events from the Google Calendar.")
app.add_typer(validate_db, name="validate-db", help="Commands to validate the database.")

@app.callback()
def main():
    """
    Initialize logging and paths before any subcommand runs.
    This runs once before any command or subcommand.
    """
    Paths.initialize(app_name="sports-calendar")
    setup_logging(config_file=Paths.LOG_CONFIG_FILE, log_dir=Paths.LOG_DIR)
    Paths.log_paths(logger=logger)
