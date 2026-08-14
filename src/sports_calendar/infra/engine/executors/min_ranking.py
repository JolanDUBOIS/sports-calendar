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

    #: How far back to look for a table anyone has played in. The current season
    #: and the one before it — beyond that the standings describe a squad that no
    #: longer exists.
    _SEASONS_TO_TRY = 2

    @classmethod
    def _extract_total_standings(cls, competition: Competition | None) -> tuple[Standings | None, str | None]:
        """ The most recent league table that has actually been played.

        A season publishes its table before a ball is kicked: eighteen rows, every
        one on zero points, ordered by nothing in particular. It is not missing,
        so it passes every emptiness check — and "the top 5 of Ligue 1" in August
        came back as *Auxerre, Angers, Monaco, Troyes, Lorient* instead of PSG,
        Lens, Lille, Lyon, Marseille. The filter looked like it worked and quietly
        followed the wrong five clubs.

        So a table counts only once someone has played in it, and until then the
        previous season's final table stands in. That is also the honest answer to
        "who are the top 5 right now" in the week before a season starts.
        """
        if competition is None:
            return None, "not found"

        if not competition.seasons:
            return None, "no seasons"

        reason = "no standings"
        for season in competition.seasons[:cls._SEASONS_TO_TRY]:
            try:
                standings = season.standings
            except ProviderNotFoundError:
                reason = "standings not found"
                continue

            total = next((s for s in (standings or []) if s.kind == "total"), None)
            if total is None:
                reason = "no total standings"
                continue

            if not cls._has_been_played(total):
                reason = "season not started"
                continue

            return total, None

        return None, reason

    @staticmethod
    def _has_been_played(standings: Standings) -> bool:
        """ Whether anyone in this table has played a match yet.

        `matches` is 0 for every row of an unstarted season and 34 or 38 for a
        finished one, so one played match anywhere is enough to trust the order.
        """
        return any((getattr(entry, "matches", 0) or 0) > 0 for entry in standings.entries)
