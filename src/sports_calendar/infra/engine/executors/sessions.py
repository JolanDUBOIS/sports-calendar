import logging

from sportindex import Competition, EventCollection, SportClient, StageEvent

from sports_calendar.core.selection import SessionsFilterFields

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class SessionsExecutor(BaseExecutor[SessionsFilterFields]):
    """ Executor for sessions filters. """

    @classmethod
    def fetch(cls, filter_fields: SessionsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the sessions criteria using the provided SportClient. """
        events = EventCollection()
        competition = client.get(filter_fields.competition_id, Competition)
        if competition is None:
            logger.warning(f"Competition with ID {filter_fields.competition_id} not found. Returning empty event collection.")
            return events
        main_events = competition.seasons[0].get_fixtures()
        for event in main_events:
            if not isinstance(event, StageEvent):
                logger.warning(f"Skipping non-stage event {event!r} while fetching sessions events.")
                continue
            for substage in event.substages:
                if substage.tier in filter_fields.sessions:
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
            if not isinstance(event, StageEvent):
                logger.warning(f"Skipping non-stage event {event!r} while filtering sessions events.")
                continue
            if event.competition and event.competition.id == filter_fields.competition_id and event.tier in filter_fields.sessions:
                filtered_events.add(event)
        return filtered_events
