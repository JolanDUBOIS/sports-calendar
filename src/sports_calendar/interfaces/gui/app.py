from .pages import root, selection_details

from . import logger
from .pages import selections
from sports_calendar.core import Paths
from sports_calendar.application.selection import SelectionService


def create_app():
    logger.info("Initializing DB & Selection Service...")
    SelectionService.initialize_registry()

    logger.info("Creating Sports Calendar GUI application...")
    root.register()
    selections.register()
    selection_details.register()
