from nicegui import ui

from . import logger


def register():
    logger.info("Registering root page...")
    
    @ui.page('/')
    def index():
        ui.label('Redirecting...')
        ui.timer(0, lambda: ui.navigate.to('/selections'), once=True)
