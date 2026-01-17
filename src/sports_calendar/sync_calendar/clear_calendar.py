from . import logger
from .config import Secrets
from .google_calendar import GoogleCalendarManager


def clear_calendar(
    name: str = "dev",
    scope: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    **kwargs
):
    """ Clear events from the Google Calendar based on the specified scope. """
    logger.info(f"Clearing calendar events with scope: {scope}")

    gcal_id = Secrets().get_gcal_id(name)
    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.clear_calendar(scope=scope, date_from=date_from, date_to=date_to, verbose=True)

    logger.info(f"Calendar events cleared successfully.")
