import logging

from sportindex import Competition, EventCollection, SportClient

from sports_calendar.core.selection import CompetitionsFilterFields

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class CompetitionsExecutor(BaseExecutor[CompetitionsFilterFields]):
    """ Executor for competitions filters. """

    @classmethod
    def fetch(cls, filter_fields: CompetitionsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the competitions criteria using the provided SportClient. """
        events = EventCollection()
        for competition_id in filter_fields.competition_ids:
            competition = client.get(Competition, competition_id)
            if competition is None:
                logger.warning(f"Competition with ID {competition_id} not found. Skipping.")
                continue
            events |= competition.seasons[0].get_fixtures()
        return events

    @classmethod
    def apply(cls, filter_fields: CompetitionsFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the competitions filter to the provided events. """
        return cls._filter_events_by_competitions(events, filter_fields)

    @staticmethod
    def _filter_events_by_competitions(events: EventCollection, filter_fields: CompetitionsFilterFields) -> EventCollection:
        """ Filter events based on the competitions criteria defined in the filter fields. """
        filtered_events = EventCollection()
        for event in events:
            if event.competition and event.competition.id in filter_fields.competition_ids:
                filtered_events.append(event)
        return filtered_events
