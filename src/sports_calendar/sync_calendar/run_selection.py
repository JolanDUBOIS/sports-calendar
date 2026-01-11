from . import logger
from .config import Secrets
from .calendar import SportsCalendar
from .events import SportsEventCollection
from .transformer import EventTransformer
from .google_calendar import GoogleCalendarManager
from sports_calendar.core import Paths
from sports_calendar.core.db import setup_repo_path
from sports_calendar.core.selection import SelectionManager, SelectionApplier


def run_selection(
    key: str = "dev",
    dry_run: bool = False,
    **kwargs
):
    """ TODO """
    logger.info(f"Running selection for selection {key}.")

    setup_repo_path(Paths.DB_DIR)

    selection = SelectionManager().get(key)
    views = SelectionApplier.apply(selection)
    events = SportsEventCollection()
    for view in views:
        events.extend(EventTransformer.transform(view))
    events.drop_duplicates(inplace=True)

    logger.info(f"Total events selected: {len(events)}")
    logger.debug(f"Selected events:\n{events}")

    calendar = SportsCalendar()
    calendar.add_events(events)

    if dry_run:
        logger.info("Dry run mode is enabled. No events will be added to the google calendar.")
        logger.debug(f"Calendar events to be added:\n{calendar}")
        return

    add_calendar_google(
        calendar=calendar,
        gcal_id=Secrets().get_gcal_id(key),
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
