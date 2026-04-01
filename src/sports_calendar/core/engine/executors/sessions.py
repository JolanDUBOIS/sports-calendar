from sportindex import SportClient, EventCollection

from . import logger
from .base import BaseExecutor
from sports_calendar.core.selection import SessionsFilterFields


class SessionsExecutor(BaseExecutor[SessionsFilterFields]):
    """ Executor for sessions filters. """

    @classmethod
    def fetch(cls, filter_fields: SessionsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the sessions criteria using the provided SportClient. """
        events = EventCollection()
        competition = client.get_competition(filter_fields.competition_id)
        if competition is None:
            logger.warning(f"Competition with ID {filter_fields.competition_id} not found. Returning empty event collection.")
            return events
        main_events = competition.seasons[0].get_fixtures()
        for event in main_events:
            for substage in event.substages:
                if substage.name in filter_fields.sessions:
                    events.append(substage)
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
                    filtered_events.append(event)
        return filtered_events
