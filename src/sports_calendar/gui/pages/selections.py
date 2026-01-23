from nicegui import ui

from . import logger
from ..components.modals import Modal
from ..components.selection_list import selection_list
from sports_calendar.core.selection import SelectionService


def register():
    logger.info("Registering selections page...")

    @ui.page('/selections')
    def selections_page():
        logger.debug("Loading selections page...")
        with ui.row().classes('justify-center items-center w-full'):
            ui.label('Your Selections').classes('text-h4 font-bold mx-auto')
        selection_list()

        def on_create_click():
            logger.debug("Create New Selection clicked")
            Modal(
                title='Create New Selection',
                message='Enter the name of your new selection',
                min_width='500px',
                max_width='800px',
                reload_on_confirm=True
            ).open(
                on_confirm=SelectionService.add_empty_selection,
                fields=[{'key': 'name', 'label': 'Name', 'type': 'text'}]
            )

        ui.button('Create', on_click=on_create_click).classes('mt-6 mx-auto block')
