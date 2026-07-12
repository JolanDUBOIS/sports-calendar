import logging

from sports_calendar.application.selection import SelectionService

from .pages import root, selection_details, selections

logger = logging.getLogger(__name__)


def create_app():
    logger.info("Initializing DB & Selection Service...")
    SelectionService.initialize_registry()

    logger.info("Creating Sports Calendar GUI application...")
    root.register()
    selections.register()
    selection_details.register()
