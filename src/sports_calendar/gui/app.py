from . import logger
from .pages import root, selections, selection_details
from sports_calendar.core import Paths
from sports_calendar.core.db import setup_repo_path
from sports_calendar.core.selection import SelectionService


def create_app():
    logger.info("Initializing DB & Selection Service...")
    setup_repo_path(Paths.DB_DIR)
    SelectionService.initialize_registry()

    logger.info("Creating Sports Calendar GUI application...")
    root.register()
    selections.register()
    selection_details.register()
