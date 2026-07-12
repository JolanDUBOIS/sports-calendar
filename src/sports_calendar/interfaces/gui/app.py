import logging
from pathlib import Path

from fastapi.responses import RedirectResponse
from nicegui import app, ui

from sports_calendar.infra.setup import init_environment

# TODO - Change to relative import
from sports_calendar.interfaces.gui.context import app_context
from sports_calendar.interfaces.gui.views import (
    selection,  # noqa: F401
    selections,  # noqa: F401
)

logger = logging.getLogger(__name__)


ASSETS_DIR = Path(__file__).resolve().parent / 'assets'

@app.get('/')
def home_route():
    return RedirectResponse('/selections')

if __name__ in {"__main__", "__mp_main__"}:
    init_environment()
    app_context.boot()
    ui.run(title='Sports Calendar', favicon=ASSETS_DIR / 'app-icon.svg')
