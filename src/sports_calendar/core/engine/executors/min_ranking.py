from sportindex import SportClient, EventCollection, Competition, Standings

from . import logger
from .base import BaseExecutor
from .teams import TeamsExecutor
from sports_calendar.core.selection import MinRankingFilterFields, TeamsFilterFields


class MinRankingExecutor(BaseExecutor[MinRankingFilterFields]):
    """ Executor for minimum ranking filters. """

    @classmethod
    def fetch(cls, filter_fields: MinRankingFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events that meet the minimum ranking criteria using the provided SportClient. """
        teams_filter_fields = cls._transform_to_teams_filter_fields(filter_fields, client)
        return TeamsExecutor.fetch(teams_filter_fields, client)

    @classmethod
    def apply(cls, filter_fields: MinRankingFilterFields, events: EventCollection) -> EventCollection:
        """ Apply the minimum ranking filter to the provided events. """
        teams_filter_fields = cls._transform_to_teams_filter_fields(filter_fields, client=None)
        return TeamsExecutor.apply(teams_filter_fields, events)

    @classmethod
    def _transform_to_teams_filter_fields(cls, filter_fields: MinRankingFilterFields, client: SportClient) -> TeamsFilterFields:
        """ Transform MinRankingFilterFields into TeamsFilterFields by fetching the relevant teams based on the specified ranking and competitions. """
        team_ids: set[str] = set()
        for comp_id in filter_fields.competition_ids:
            competition = client.get_competition(comp_id)
            total_standings, reason = cls._extract_total_standings(competition)

            if total_standings is None:
                logger.warning(f"Could not extract total standings for competition {comp_id}: {reason}")
                continue

            for entry in total_standings.entries:
                if entry.position <= filter_fields.ranking:
                    team_ids.add(entry.competitor.id)

        return TeamsFilterFields(team_ids=sorted(team_ids), selection_rule=filter_fields.selection_rule)

    @staticmethod
    def _extract_total_standings(competition: Competition | None) -> tuple[Standings | None, str | None]:
        """ Helper method to extract total standings from a competition, with error handling. """
        if competition is None:
            return None, "not found"

        if not competition.seasons:
            return None, "no seasons"

        standings = competition.seasons[0].standings
        if standings is None:
            return None, "no standings"

        total = standings.get(kind="total")
        if total is None:
            return None, "no total standings"

        return total, None
