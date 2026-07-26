from __future__ import annotations

import logging
from typing import Protocol

from sportindex import Competition, Competitor, Sport, SportClient

logger = logging.getLogger(__name__)


# ==== Protocols ====

class FilterSearchProvider(Protocol):
    def search_competition(self, query: str, sport_id: int) -> dict[int, str]: ...

    def search_competitor(self, query: str, sport_id: int) -> dict[int, str]: ...

    def get_competition_option(self, competition_id: int) -> str: ...

    def get_competitor_option(self, competitor_id: int) -> str: ...

    def get_competition_options(self, competition_ids: list[int]) -> dict[int, str]: ...

    def get_competitor_options(self, competitor_ids: list[int]) -> dict[int, str]: ...


# ==== Formatting Helpers ====

def format_entity_label(entity: object, fallback: int) -> str:
    return str(getattr(entity, "short_name", None) or getattr(entity, "name", fallback))


# ==== Direct Lookup Helpers ====

def get_sport_name(client: SportClient, sport_id: str) -> str:
    sport = client.get(Sport.encode_id(sport_id), Sport)
    return sport.name.capitalize()

def get_valid_sport_ids(client: SportClient) -> list[int]:
    return [sport.id for sport in client.list(Sport)]


# ==== Provider Implementation ====

class SportIndexFilterSearchProvider:
    def __init__(self, client: SportClient):
        self._client = client

    def search_competition(self, query: str, sport_id: int) -> dict[int, str]:
        target_sport_id = Sport.encode_id(sport_id)
        competitions = self._client.search(Competition, query=query, max_results=50)
        return {
            competition.id: format_entity_label(competition, competition.id)
            for competition in competitions
            if getattr(getattr(competition, "sport", None), "id", target_sport_id) == target_sport_id
        }

    def search_competitor(self, query: str, sport_id: int) -> dict[int, str]:
        target_sport_id = Sport.encode_id(sport_id)
        competitors = self._client.search(Competitor, query=query, max_results=50)
        return {
            competitor.id: format_entity_label(competitor, competitor.id)
            for competitor in competitors
            if getattr(getattr(competitor, "sport", None), "id", target_sport_id) == target_sport_id
        }

    def get_competition_option(self, competition_id: int) -> str:
        try:
            competition = self._client.get(Competition, competition_id)
            return format_entity_label(competition, competition_id)
        except Exception as e:
            logger.error("Error occurred while fetching competition with ID %d: %s", competition_id, str(e))
            return str(competition_id)

    def get_competitor_option(self, competitor_id: int) -> str:
        try:
            competitor = self._client.get(Competitor, competitor_id)
            return format_entity_label(competitor, competitor_id)
        except Exception as e:
            logger.error("Error occurred while fetching competitor with ID %d: %s", competitor_id, str(e))
            return str(competitor_id)

    def get_competition_options(self, competition_ids: list[int]) -> dict[int, str]:
        if len(competition_ids) > 30:
            logger.warning("Received a large number of competition IDs (%d). This may impact performance.", len(competition_ids))
        competitions = {}
        for comp_id in competition_ids:
            try:
                competitions[comp_id] = self.get_competition_option(comp_id)
            except Exception as e:
                logger.error("Error occurred while fetching competition with ID %d: %s", comp_id, str(e))
        return competitions

    def get_competitor_options(self, competitor_ids: list[int]) -> dict[int, str]:
        if len(competitor_ids) > 30:
            logger.warning("Received a large number of competitor IDs (%d). This may impact performance.", len(competitor_ids))
        competitors = {}
        for comp_id in competitor_ids:
            try:
                competitors[comp_id] = self.get_competitor_option(comp_id)
            except Exception as e:
                logger.error("Error occurred while fetching competitor with ID %d: %s", comp_id, str(e))
        return competitors
