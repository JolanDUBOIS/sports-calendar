from . import logger
from .data_access import get_clean_matches
from .calendar import (
    FootballCalendar,
    MatchesDetails,
    GoogleCalendarManager
)
from .orchestration import AppConfig


def run_selection(repo: str = "prod", name: str = "dev", dry_run: bool = False, preset: str = "default", **kwargs):
    """ TODO """
    logger.info(f"Running selection for selection {name}.")

    app_config = AppConfig(repo=repo)
    selection = app_config.get_selection(name)
    selection_params = app_config.get_selection_params(key=preset)
    gcal_id = app_config.get_gcal_id(key=name)

    if selection is None:
        logger.error(f"Selection {name} not found in the configuration.")
        raise ValueError(f"Selection {name} not found in the configuration.")
    if selection_params is None:
        logger.error(f"Selection parameters for {preset} not found in the configuration.")
        raise ValueError(f"Selection parameters for {preset} not found in the configuration.")
    if gcal_id is None:
        logger.error(f"Google Calendar ID for {selection.username} not found in the secrets.")
        raise ValueError(f"Google Calendar ID for {selection.username} not found in the secrets.")

    matches = selection.get_matches()
    matches = get_clean_matches(matches)
    matches_details = MatchesDetails.from_dataframe(matches)

    football_calendar = FootballCalendar()
    football_calendar.add_matches(matches_details)

    if dry_run:
        logger.info("Dry run mode is enabled. No events will be added to the google calendar.")
        return

    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.add_calendar(football_calendar.calendar, verbose=True)

    logger.info(f"Selection {name} has been successfully processed and added to the google calendar.")

def clear_calendar(scope: str = 'future', key: str = "dev", **kwargs):
    """ Clear events from the Google Calendar based on the specified scope. """
    logger.info(f"Clearing calendar events with scope: {scope}")

    gcal_id = AppConfig().get_gcal_id(key=key)
    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.clear_calendar(scope=scope, verbose=True)

    logger.info(f"Calendar events cleared successfully.")
