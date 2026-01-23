from nicegui import ui

from . import logger
from .selection_card import selection_card
from sports_calendar.core.selection import SelectionService


def selection_list():
    selections = SelectionService.get_all_selections()
    logger.debug(f"Loaded {len(selections)} selections for selection list.")

    with ui.column().classes('w-full p-4 gap-4'):
        for selection in selections:
            selection_card(selection)
