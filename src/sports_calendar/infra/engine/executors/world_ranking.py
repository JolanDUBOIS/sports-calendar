import logging

from sportindex import Competitor, EventCollection, Sport, SportClient
from sportindex.exceptions import ProviderNotFoundError

from sports_calendar.core.selection import (
    CompetitorsFilterFields,
    WorldRankingFilterFields,
)

from .base import BaseExecutor
from .competitors import CompetitorsExecutor

logger = logging.getLogger(__name__)


class WorldRankingExecutor(BaseExecutor[WorldRankingFilterFields]):
    """ Executor for world-ranking filters.

    The sibling of `MinRankingExecutor`. Both answer "follow whoever is near
    the top", and both do it by resolving to a set of competitors and handing
    off to `CompetitorsExecutor`. They differ in where "the top" is read from:
    min-ranking reads a season's league table, this reads a governing body's
    standing order — ATP, WTA, FIFA, World Rugby. Sports without a league table
    have only the latter, which is why tennis needs this one.

    Because the ranking is re-read every time the calendar is built, a filter
    written once keeps following whoever currently holds those positions.
    """

    @classmethod
    def fetch(cls, filter_fields: WorldRankingFilterFields, client: SportClient) -> EventCollection:
        """ Fetch events for the competitors currently ranked above the cut-off. """
        competitors_filter_fields = cls._transform_to_competitors_filter_fields(filter_fields, client)
        return CompetitorsExecutor.fetch(competitors_filter_fields, client)

    @classmethod
    def apply(cls, filter_fields: WorldRankingFilterFields, events: EventCollection, client: SportClient) -> EventCollection:
        """ Keep only events involving the competitors ranked above the cut-off. """
        competitors_filter_fields = cls._transform_to_competitors_filter_fields(filter_fields, client)
        return CompetitorsExecutor.apply(competitors_filter_fields, events, client)

    @classmethod
    def _transform_to_competitors_filter_fields(
        cls, filter_fields: WorldRankingFilterFields, client: SportClient
    ) -> CompetitorsFilterFields:
        """ Resolve the ranking into the set of competitors above the cut-off. """
        rankings = cls._get_rankings(filter_fields, client)
        if rankings is None:
            return CompetitorsFilterFields(competitor_ids=[], selection_rule=filter_fields.selection_rule)

        try:
            entries = rankings.entries
        except ValueError:
            # sport-index builds every row up front, so one row naming neither a
            # competitor nor a competition takes the whole table down with it.
            # Observed on the Rugby League ranking. Nothing to salvage from here.
            logger.exception(
                f"Ranking {filter_fields.ranking_id} for sport {filter_fields.sport_id} "
                f"could not be parsed. Skipping ranking filter."
            )
            return CompetitorsFilterFields(competitor_ids=[], selection_rule=filter_fields.selection_rule)

        competitor_ids: set[str] = set()
        for entry in entries:
            if entry.position > filter_fields.ranking:
                continue
            # Some rankings order competitions rather than competitors; only
            # the latter can be followed.
            if not isinstance(entry.entity, Competitor):
                continue
            competitor_ids.add(entry.entity.id)

        if not competitor_ids:
            logger.warning(
                f"Ranking {filter_fields.ranking_id} yielded no competitors "
                f"at or above position {filter_fields.ranking}."
            )

        return CompetitorsFilterFields(
            competitor_ids=sorted(competitor_ids),
            selection_rule=filter_fields.selection_rule,
        )

    @staticmethod
    def _get_rankings(filter_fields: WorldRankingFilterFields, client: SportClient):
        """ The ranking table named by the filter, or None with a reason logged.

        Rankings are not addressable entities in sport-index — they are only
        reachable through the sport that owns them, hence the lookup by sport
        followed by a search for the requested ranking id.
        """
        try:
            sport = client.get(Sport.encode_id(filter_fields.sport_id), Sport)
        except ProviderNotFoundError:
            logger.warning(f"Sport {filter_fields.sport_id} not found. Skipping ranking filter.")
            return None

        if sport is None:
            logger.warning(f"Sport {filter_fields.sport_id} not found. Skipping ranking filter.")
            return None

        try:
            available = sport.get_rankings()
        except ProviderNotFoundError:
            logger.warning(f"No rankings available for sport {filter_fields.sport_id}.")
            return None

        rankings = next((r for r in available if r.id == filter_fields.ranking_id), None)
        if rankings is None:
            logger.warning(
                f"Ranking {filter_fields.ranking_id} is not published for sport "
                f"{filter_fields.sport_id}. Skipping ranking filter."
            )
        return rankings
