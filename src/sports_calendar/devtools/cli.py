import typer

from . import logger
from sports_calendar.core.selection import SelectionService


dev_tools = typer.Typer(help="Development tools for the sports calendar application.")

@dev_tools.command()
def validate_selections():
    """ Validate the selections files and rewrites them into a standardized format if possible. """
    SelectionService.initialize_registry() # Loading the selections validates them
    logger.info("All selection files are valid.")
    selections = SelectionService.get_all_selections()
    for sel in selections:
        try:
            logger.info(f"Rewriting selection: {sel.name}")
            SelectionService.replace_selection(sel) # Rewriting the selection to standardize the format
        except Exception:
            logger.exception(f"Failed to rewrite selection: {sel.name}")
