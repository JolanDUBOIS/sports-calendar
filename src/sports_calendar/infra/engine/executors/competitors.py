import logging

from sportindex import Competitor, Event, EventCollection, SportClient

from sports_calendar.core import raw_entity_id
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
        """ Check if the event matches the selection rule defined in the filter fields.

        Comparison is on raw ids, not on the ids as stored. A team picked from
        the search box is saved as `team:1644`, while the same club appears on
        its own fixtures as `t-cpt:1644` — comparing those as strings matched
        nothing, so every "follow this team" filter came back empty.
        """
        logger.debug(f"Checking event {event.id} against selection rule {filter_fields.selection_rule}")
        if not event.competitors:
            return False

        wanted = {raw_entity_id(competitor_id) for competitor_id in filter_fields.competitor_ids}
        present = {
            raw_entity_id(competitor.id)
            for competitor in (event.competitors.home, event.competitors.away)
            if competitor is not None
        }

        if filter_fields.selection_rule.rule == Rule.ANY:
            return bool(present & wanted)
        if filter_fields.selection_rule.rule == Rule.BOTH:
            return present.issubset(wanted)
        if filter_fields.selection_rule.rule == Rule.OPPONENT:
            reference_id = raw_entity_id(filter_fields.selection_rule.reference or "")
            if reference_id in present:
                others = present - {reference_id}
                return bool(others & wanted)
        return False
