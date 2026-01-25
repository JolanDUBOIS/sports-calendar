from nicegui import ui

from . import logger
from ..components.modals import Modal
from ..components.items_list import items_list
from ..components.filters.fields import SelectField, Choice
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
        def on_create_item_click():
            logger.debug(f"Create New Item clicked for selection: {sname}")

            Modal(
                title='Create New Selection Item',
                message='Enter the sport for your new selection item',
                min_width='500px',
                max_width='800px',
                reload_on_confirm=True
            ).open(
                on_confirm=lambda sport: SelectionService.add_empty_item(sname, sport),
                fields=[SelectField(key='sport', label='Sport', options=[Choice(value="football", label="Football"), Choice(value="f1", label="F1")])] # TODO - Improve that ofc
            )

        ui.button('+', on_click=on_create_item_click).props('small round').classes('mt-6 mx-auto block')
