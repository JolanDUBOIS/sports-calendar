from sportindex import SportClient, EventCollection, Competition

from . import logger
from .base import BaseExecutor
from sports_calendar.core.selection import SessionsFilterFields


class SessionsExecutor(BaseExecutor[SessionsFilterFields]):
    """ Executor for sessions filters. """

    @classmethod
    def fetch(cls, filter_fields: SessionsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the sessions criteria using the provided SportClient. """
        events = EventCollection()
        competition = client.get(Competition, filter_fields.competition_id)
        if competition is None:
            logger.warning(f"Competition with ID {filter_fields.competition_id} not found. Returning empty event collection.")
            return events
        main_events = competition.seasons[0].get_fixtures()
        for event in main_events:
            for substage in event.substages:
                # TODO - Use stage type when released on SportIndex instead of the name !!!!
                if substage.name in filter_fields.sessions: # TODO - Not just "in" but also "in" any of the sessions (e.g. "Qualifying" is fine for "Qualifying 1", "Qualifying 2", etc.)
                    events.add(substage)
        return events

    @classmethod
    def apply(cls, filter_fields: SessionsFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the sessions filter to the provided events. """
        return cls._filter_events_by_sessions(events, filter_fields)

    @staticmethod
    def _filter_events_by_sessions(events: EventCollection, filter_fields: SessionsFilterFields) -> EventCollection:
        """ Filter events based on the sessions criteria defined in the filter fields. """
        filtered_events = EventCollection()
        for event in events:
            if event.competition and event.competition.id == filter_fields.competition_id and event.name in filter_fields.sessions:
                    filtered_events.add(event)
        return filtered_events
