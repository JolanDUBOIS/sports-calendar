from nicegui import ui

from . import logger
from ..components.items_list import items_list
from sports_calendar.core.selection import SelectionService


def register():
    logger.info("Registering selection details page...")

    @ui.page('/selections/{sname}')
    def selection_details_page(sname: str):
        logger.debug(f"Loading details page for selection: {sname}")
        
        # Title
        with ui.row().classes('justify-center items-center w-full'):
            ui.label(f'Selection - "{sname}"').classes('text-h4 font-bold mx-auto')

        # Items list
        selection = SelectionService.get_selection(sname)
        items_list(selection)

        # Create button
        def on_create_click():
            logger.debug(f"Create New Item clicked for selection: {sname}")

        ui.button('Create New Item', on_click=on_create_click).classes('mt-6 mx-auto block')
