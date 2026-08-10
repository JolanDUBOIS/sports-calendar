import logging

from sports_calendar.core.calendar import SportsCalendar
from sports_calendar.infra.google_calendar import GoogleCalendarManager, Secrets

from .build_calendar import build_calendar

logger = logging.getLogger(__name__)


def run_selection(
    name: str = "dev",
    dry_run: bool = False,
    **kwargs
):
    """ Resolve a selection and push the resulting events to Google Calendar.

    Backend-only workflow: requires the `backend` extra.
    """
    logger.info(f"Running selection for selection {name}.")

    calendar = build_calendar(name)

    if dry_run:
        logger.info("Dry run mode is enabled. No events will be added to the google calendar.")
        logger.debug(f"Calendar events to be added:\n{calendar}")
        return

    add_calendar_google(
        calendar=calendar,
        gcal_id=Secrets().get_gcal_id(name),
        scope='future',
        verbose=kwargs.get('verbose', False)
    )

def add_calendar_google(
    calendar: SportsCalendar,
    gcal_id: str,
    scope: str = 'future',
    verbose: bool = False
):
    """ Add events from the SportsCalendar to the Google Calendar. """
    logger.info("Adding events to Google Calendar.")

    google_cal_manager = GoogleCalendarManager.from_defaults(gcal_id)
    google_cal_manager.clear_calendar(scope=scope, verbose=verbose)
    google_cal_manager.add_calendar(calendar.calendar, scope=scope, verbose=verbose)

    logger.info("Events have been successfully added to the Google Calendar.")
