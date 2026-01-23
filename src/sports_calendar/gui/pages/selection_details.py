from nicegui import ui

from . import logger


def register():
    logger.info("Registering selection details page...")

    @ui.page('/selections/{sname}')
    def selection_details_page(sname: str):
        logger.debug(f"Loading details page for selection: {sname}")
        ui.label(f'Details for Selection: {sname}').classes('text-h4 font-bold mx-auto')
