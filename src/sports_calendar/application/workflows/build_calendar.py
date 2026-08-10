import logging

from sportindex import SportClient

from sports_calendar.application.selection import SelectionService
from sports_calendar.core.calendar import (
    EVENT_TYPE_MAP,
    SportsCalendar,
    SportsEventCollection,
)
from sports_calendar.infra.engine import Resolver

logger = logging.getLogger(__name__)


def build_calendar(name: str = "dev") -> SportsCalendar:
    """ Resolve a selection into a SportsCalendar.

    Shared workflow: available to both the backend and the UI. Must stay free of
    any Google Calendar dependency so a UI-only install can import it.
    """
    logger.info(f"Building calendar for selection {name}.")

    SelectionService.initialize_registry()

    selection = SelectionService.get_selection(name)
    client = SportClient()
    resolved_collections = Resolver.resolve_selection(selection, client)

    events = SportsEventCollection()
    for collection, sport_id in resolved_collections:
        event_cls = EVENT_TYPE_MAP.get(sport_id)
        if not event_cls:
            raise ValueError(f"Unsupported sport id {sport_id} for event transformation.")
        events += SportsEventCollection.from_sport_index_collection(collection, event_cls)

    events.drop_duplicates(inplace=True)

    logger.info(f"Total events selected: {len(events)}")
    logger.debug(f"Selected events:\n{events}")

    calendar = SportsCalendar()
    calendar.add_events(events)
    return calendar
