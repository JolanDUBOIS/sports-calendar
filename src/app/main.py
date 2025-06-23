from __future__ import annotations
from typing import TYPE_CHECKING

from . import logger
from .data_access import get_clean_matches
from .calendar import (
    FootballCalendar,
    MatchesDetails,
    GoogleCalendarManager
)
from .selection import SelectionManager
from src.config.manager import secrets
if TYPE_CHECKING:
    from src.config.base_config import Repository


def run_selection(key: str = "dev", dry_run: bool = False, **kwargs):
    """ TODO """
    logger.info(f"Running selection for selection {key}.")

    selection = SelectionManager().get_selection(key)
    gcal_id = secrets.get_gcal_id(key)

    matches = selection.get_matches()
    matches = get_clean_matches(matches)
    matches_details = MatchesDetails.from_dataframe(matches)

    football_calendar = FootballCalendar()
    football_calendar.add_matches(matches_details)

    if dry_run:
        logger.info("Dry run mode is enabled. No events will be added to the google calendar.")
        return

    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.clear_calendar(scope='future', verbose=True)
    google_cal_manager.add_calendar(football_calendar.calendar, scope='future', verbose=True)

    logger.info(f"Selection {key} has been successfully processed and added to the google calendar.")

def clear_calendar(key: str = "dev", scope: str | None = None, date_from: str | None = None, date_to: str | None = None, **kwargs):
    """ Clear events from the Google Calendar based on the specified scope. """
    logger.info(f"Clearing calendar events with scope: {scope}")

    gcal_id = secrets.get_gcal_id(key)
    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.clear_calendar(scope=scope, date_from=date_from, date_to=date_to, verbose=True)

    logger.info(f"Calendar events cleared successfully.")
