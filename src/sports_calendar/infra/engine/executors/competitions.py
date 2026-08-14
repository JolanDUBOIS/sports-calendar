import logging

from sportindex import Competition, EventCollection, SportClient
from sportindex.exceptions import ProviderNotFoundError

from sports_calendar.core import EntityId, raw_entity_id
from sports_calendar.core.rounds import keeps_event_from, rounds_for_competition
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
            events |= cls.fetch_current_fixtures(competition_id, client)
        return cls._filter_events_from_round(events, filter_fields, client)

    @classmethod
    def apply(cls, filter_fields: CompetitionsFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the competitions filter to the provided events. """
        matching = cls._filter_events_by_competitions(events, filter_fields)
        return cls._filter_events_from_round(matching, filter_fields, client)

    @staticmethod
    def _filter_events_by_competitions(events: EventCollection, filter_fields: CompetitionsFilterFields) -> EventCollection:
        """ Filter events based on the competitions criteria defined in the filter fields. """
        wanted = {raw_entity_id(competition_id) for competition_id in filter_fields.competition_ids}
        filtered_events = EventCollection()
        for event in events:
            if event.competition and raw_entity_id(event.competition.id) in wanted:
                filtered_events.add(event)
        return filtered_events

    @classmethod
    def _filter_events_from_round(
        cls, events: EventCollection, filter_fields: CompetitionsFilterFields, client: SportClient
    ) -> EventCollection:
        """ Keep the chosen round and everything after it, per competition.

        No round chosen means the whole competition, which is the default and
        the common case.

        "After" is each competition's own order — `Season.rounds` comes back in
        the order rounds are played — so one choice means the right thing in
        every competition without any ranking of ours. A competition that does
        not publish the chosen round contributes nothing, since "from here on"
        has no meaning there.
        """
        if not filter_fields.from_round:
            return events

        ladders = {
            raw_entity_id(competition_id): list(cls._ordered_rounds(competition_id, client))
            for competition_id in filter_fields.competition_ids
        }

        filtered_events = EventCollection()
        for event in events:
            competition = getattr(event, "competition", None)
            if competition is None:
                continue
            ladder = ladders.get(raw_entity_id(competition.id))
            if not ladder:
                continue
            if keeps_event_from(ladder, filter_fields.from_round, getattr(event, "round", None)):
                filtered_events.add(event)
        return filtered_events

    @staticmethod
    def _ordered_rounds(competition_id: EntityId, client: SportClient) -> dict[str, str]:
        """ A competition's rounds in playing order, or empty if unavailable. """
        try:
            competition = client.get(competition_id, Competition)
            if competition is None:
                logger.warning(f"Competition {competition_id} not found while reading rounds.")
                return {}
            return rounds_for_competition(competition)
        except ProviderNotFoundError:
            logger.warning(f"Rounds unavailable for competition {competition_id}.")
            return {}
