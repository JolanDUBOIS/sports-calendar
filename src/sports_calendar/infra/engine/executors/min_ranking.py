import logging

from sportindex import Competition, EventCollection, SportClient, Standings
from sportindex.exceptions import ProviderNotFoundError

from sports_calendar.core.selection import (
    CompetitorsFilterFields,
    MinRankingFilterFields,
)

from .base import BaseExecutor
from .competitors import CompetitorsExecutor

logger = logging.getLogger(__name__)


class MinRankingExecutor(BaseExecutor[MinRankingFilterFields]):
    """ Executor for minimum ranking filters. """

    @classmethod
    def fetch(cls, filter_fields: MinRankingFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the minimum ranking criteria using the provided SportClient. """
        competitors_filter_fields = cls._transform_to_competitors_filter_fields(filter_fields, client)
        return CompetitorsExecutor.fetch(competitors_filter_fields, client)

    @classmethod
    def apply(cls, filter_fields: MinRankingFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Apply the minimum ranking filter to the provided events. """
        competitors_filter_fields = cls._transform_to_competitors_filter_fields(filter_fields, client=client)
        return CompetitorsExecutor.apply(competitors_filter_fields, events, client)

    @classmethod
    def _transform_to_competitors_filter_fields(cls, filter_fields: MinRankingFilterFields, client: SportClient) -> CompetitorsFilterFields:
        """ Transform MinRankingFilterFields into CompetitorsFilterFields by fetching the relevant competitors based on the specified ranking and competitions. """
        competitor_ids: set[str] = set()
        for comp_id in filter_fields.competition_ids:
            competition = client.get(comp_id, Competition)
            total_standings, reason = cls._extract_total_standings(competition)

            if total_standings is None:
                logger.warning(f"Could not extract total standings for competition {comp_id}: {reason}")
                continue

            for entry in total_standings.entries:
                if entry.position <= filter_fields.ranking:
                    competitor_ids.add(entry.competitor.id)

        return CompetitorsFilterFields(competitor_ids=sorted(competitor_ids), selection_rule=filter_fields.selection_rule)

    @staticmethod
    def _extract_total_standings(competition: Competition | None) -> tuple[Standings | None, str | None]:
        """ Helper method to extract total standings from a competition, with error handling. """
        if competition is None:
            return None, "not found"

        if not competition.seasons:
            return None, "no seasons"

        try:
            standings = competition.seasons[0].standings
        except ProviderNotFoundError:
            return None, "standings not found"
        if standings is None:
            return None, "no standings"

        total = next((s for s in standings if s.kind == "total"), None)
        if total is None:
            return None, "no total standings"

        return total, None
