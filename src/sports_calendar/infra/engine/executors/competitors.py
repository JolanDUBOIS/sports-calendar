import logging

from sportindex import Competitor, Event, EventCollection, SportClient

from sports_calendar.core.selection import CompetitorsFilterFields, Rule

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class CompetitorsExecutor(BaseExecutor[CompetitorsFilterFields]):
    """ Executor for competitors filters. """

    @classmethod
    def fetch(cls, filter_fields: CompetitorsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the competitors criteria using the provided SportClient. """
        events = EventCollection()
        for competitor_id in filter_fields.competitor_ids:
            competitor = client.get(competitor_id, Competitor)
            if competitor is None:
                logger.warning(f"Competitor with ID {competitor_id} not found. Skipping.")
                continue
            events |= competitor.get_fixtures()
        return cls._filter_events_by_competitors(events, filter_fields)

    @classmethod
    def apply(cls, filter_fields: CompetitorsFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the competitors filter to the provided events. """
        return cls._filter_events_by_competitors(events, filter_fields)

    @classmethod
    def _filter_events_by_competitors(cls, events: EventCollection, filter_fields: CompetitorsFilterFields) -> EventCollection:
        """ Filter events based on the competitors criteria defined in the filter fields. """
        filtered_events = EventCollection()
        for event in events:
            if cls._matches_selection_rule(event, filter_fields):
                filtered_events.add(event)
        return filtered_events

    @staticmethod
    def _matches_selection_rule(event: Event, filter_fields: CompetitorsFilterFields) -> bool:
        """ Check if the event matches the selection rule defined in the filter fields. """
        logger.debug(f"Checking event {event.id} against selection rule {filter_fields.selection_rule}")
        if event.competitors:
            event_competitors = {event.competitors.home, event.competitors.away}
        if filter_fields.selection_rule.rule == Rule.ANY:
            return any(competitor_id in filter_fields.competitor_ids for competitor_id in [competitor.id for competitor in event_competitors])
        if filter_fields.selection_rule.rule == Rule.BOTH:
            return all(competitor_id in filter_fields.competitor_ids for competitor_id in [competitor.id for competitor in event_competitors])
        if filter_fields.selection_rule.rule == Rule.OPPONENT:
            reference_id = filter_fields.selection_rule.reference
            if reference_id in [competitor.id for competitor in event_competitors]:
                other_competitor_id = next(competitor.id for competitor in event_competitors if competitor.id != reference_id)
                return other_competitor_id in filter_fields.competitor_ids
        return False
