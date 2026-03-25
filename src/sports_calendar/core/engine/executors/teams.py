from sportindex import SportClient, EventCollection, Event

from . import logger
from .base import BaseExecutor
from sports_calendar.core.selection import TeamsFilterFields, Rule


class TeamsExecutor(BaseExecutor[TeamsFilterFields]):
    """ Executor for teams filters. """

    @classmethod
    def fetch(cls, filter_fields: TeamsFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the teams criteria using the provided SportClient. """
        events = EventCollection()
        for team_id in filter_fields.team_ids:
            competitor = client.get_competitor(team_id)
            if competitor is None:
                logger.warning(f"Competitor with ID {team_id} not found. Skipping.")
                continue
            events |= competitor.get_fixtures()
        return cls._filter_events_by_teams(events, filter_fields)

    @classmethod
    def apply(cls, filter_fields: TeamsFilterFields, events: EventCollection) -> EventCollection:
        """ Apply the teams filter to the provided events. """
        return cls._filter_events_by_teams(events, filter_fields)

    @classmethod
    def _filter_events_by_teams(cls, events: EventCollection, filter_fields: TeamsFilterFields) -> EventCollection:
        """ Filter events based on the teams criteria defined in the filter fields. """
        filtered_events = EventCollection()
        for event in events:
            if cls._matches_selection_rule(event, filter_fields):
                filtered_events.append(event)
        return filtered_events

    @staticmethod
    def _matches_selection_rule(event: Event, filter_fields: TeamsFilterFields) -> bool:
        """ Check if the event matches the selection rule defined in the filter fields. """
        logger.debug(f"Checking event {event.id} against selection rule {filter_fields.selection_rule}")
        if event.competitors:
            event_competitors = {event.competitors.home, event.competitors.away}
        if filter_fields.selection_rule.rule == Rule.ANY:
            return any(team_id in filter_fields.team_ids for team_id in [competitor.id for competitor in event_competitors])
        elif filter_fields.selection_rule.rule == Rule.BOTH:
            return all(team_id in filter_fields.team_ids for team_id in [competitor.id for competitor in event_competitors])
        elif filter_fields.selection_rule.rule == Rule.OPPONENT:
            reference_id = filter_fields.selection_rule.reference
            if reference_id in [competitor.id for competitor in event_competitors]:
                other_competitor_id = next(competitor.id for competitor in event_competitors if competitor.id != reference_id)
                return other_competitor_id in filter_fields.team_ids
        return False
