from . import logger
from ..core.calendar.calendar import SportsCalendar
from sports_calendar.core.engine import Resolver
from sports_calendar.core.selection import SelectionService
from sports_calendar.core.calendar import SportsEventCollection, EVENT_TYPE_MAP
from sports_calendar.core.google_calendar import GoogleCalendarManager, Secrets


def run_selection(
    name: str = "dev",
    dry_run: bool = False,
    **kwargs
):
    """ TODO """
    logger.info(f"Running selection for selection {name}.")

    SelectionService.initialize_registry()

    selection = SelectionService.get_selection(name)
    resolved_collections = Resolver.resolve_selection(selection)

    events = SportsEventCollection()
    for collection, sport in resolved_collections:
        event_cls = EVENT_TYPE_MAP.get(sport)
        if not event_cls:
            raise ValueError(f"Unsupported sport type {sport} for event transformation.")
        events += SportsEventCollection.from_sport_index_collection(collection, event_cls)

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
