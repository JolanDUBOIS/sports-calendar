import logging

from sportindex import EventCollection, SportClient, StageEvent
from sportindex.exceptions import DomainError, SportIndexError

from sports_calendar.core.selection import SessionsFilterFields

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class SessionsExecutor(BaseExecutor[SessionsFilterFields]):
    """ Executor for sessions filters. """

    @classmethod
    def fetch(cls, filter_fields: SessionsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the sessions criteria using the provided SportClient.

        `substage.tier` is lazy: reading it fetches the substage. One flaky
        request there used to raise straight out of the whole build — a single
        practice session cost every other event in the calendar, across every
        sport. A session that cannot be read is skipped and logged instead.
        """
        events = EventCollection()
        main_events = cls.fetch_current_fixtures(filter_fields.competition_id, client)
        for event in main_events:
            if not isinstance(event, StageEvent):
                logger.warning(f"Skipping non-stage event {event!r} while fetching sessions events.")
                continue

            try:
                substages = list(event.substages)
            except (SportIndexError, DomainError):
                logger.exception(f"Could not read sessions of stage {event.id}. Skipping it.")
                continue

            for substage in substages:
                try:
                    wanted = substage.tier in filter_fields.sessions
                except (SportIndexError, DomainError):
                    logger.exception(f"Could not read session {substage.id}. Skipping it.")
                    continue
                if wanted:
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
