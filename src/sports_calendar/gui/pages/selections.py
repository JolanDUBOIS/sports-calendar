from nicegui import ui

from . import logger
from ..components.modals import Modal
from ..components.fields import ValidatedTextField
from ..components.selections_list import selections_list
from sports_calendar.core.utils import validate
from sports_calendar.core.selection import SelectionService


def register():
    logger.info("Registering selections page...")

    @ui.page('/selections')
    def selections_page():
        logger.debug("Loading selections page...")

        # Title
        with ui.row().classes('justify-center items-center w-full'):
            ui.label('Your Selections').classes('text-h4 font-bold mx-auto')
        
        # Selection list
        selections_list()

        # Create button
        def on_create_click():
            logger.debug("Create New Selection clicked")

            def validate_name(input_value: str) -> bool | str:
                logger.debug(f"Validating selection name: {input_value}")
                validate(isinstance(input_value, str), "Input value must be a string.", logger, TypeError)
                if not input_value.strip():
                    return "Name cannot be empty."
                if SelectionService.selection_exists(input_value.strip()):
                    return "A selection with this name already exists."
                return True

            Modal(
                title='Create New Selection',
                message='Enter the name of your new selection',
                min_width='500px',
                max_width='800px',
                reload_on_confirm=True
            ).open(
                on_confirm=SelectionService.add_empty_selection,
                fields=[ValidatedTextField(key='name', label='Name', validator=validate_name)]
            )

        ui.button('Create', on_click=on_create_click).classes('mt-6 mx-auto block')
