from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

from sportindex import Competition, Competitor, Sport, SportClient

if TYPE_CHECKING:
    from sports_calendar.core import EntityId

logger = logging.getLogger(__name__)


# ==== Protocols ====

class FilterSearchProvider(Protocol):
    def search_competition(self, query: str, sport_id: int) -> dict[EntityId, str]: ...

    def search_competitor(self, query: str, sport_id: int) -> dict[EntityId, str]: ...

    def get_competition_option(self, competition_id: EntityId) -> str: ...

    def get_competitor_option(self, competitor_id: EntityId) -> str: ...

    def get_competition_options(self, competition_ids: list[EntityId]) -> dict[EntityId, str]: ...

    def get_competitor_options(self, competitor_ids: list[EntityId]) -> dict[EntityId, str]: ...


# ==== Formatting Helpers ====

def format_entity_label(entity: object | None, fallback: EntityId) -> str:
    """ Human-readable label for an entity, falling back to its raw ID. """
    return str(getattr(entity, "short_name", None) or getattr(entity, "name", fallback))


# ==== Direct Lookup Helpers ====

def get_sport_name(client: SportClient, sport_id: int) -> str:
    """ Display name of a sport.

    `sport_id` is a raw sport-index sport id (as stored on SelectionItem), not an
    EntityId — it gets encoded here.
    """
    sport = client.get(Sport.encode_id(sport_id), Sport)
    if sport is None:
        logger.warning("No sport found for id %s", sport_id)
        return str(sport_id)
    return sport.name.capitalize()


# ==== Provider Implementation ====

class SportIndexFilterSearchProvider:
    def __init__(self, client: SportClient):
        self._client = client

    def search_competition(self, query: str, sport_id: int) -> dict[EntityId, str]:
        target_sport_id = Sport.encode_id(sport_id)
        competitions = self._client.search(Competition, query=query, max_results=50)
        return {
            competition.id: format_entity_label(competition, competition.id)
            for competition in competitions
            if getattr(getattr(competition, "sport", None), "id", target_sport_id) == target_sport_id
        }

    def search_competitor(self, query: str, sport_id: int) -> dict[EntityId, str]:
        target_sport_id = Sport.encode_id(sport_id)
        competitors = self._client.search(Competitor, query=query, max_results=50)
        return {
            competitor.id: format_entity_label(competitor, competitor.id)
            for competitor in competitors
            if getattr(getattr(competitor, "sport", None), "id", target_sport_id) == target_sport_id
        }

    def get_competition_option(self, competition_id: EntityId) -> str:
        return self._get_option(competition_id, Competition)

    def get_competitor_option(self, competitor_id: EntityId) -> str:
        return self._get_option(competitor_id, Competitor)

    def get_competition_options(self, competition_ids: list[EntityId]) -> dict[EntityId, str]:
        return self._get_options(competition_ids, Competition)

    def get_competitor_options(self, competitor_ids: list[EntityId]) -> dict[EntityId, str]:
        return self._get_options(competitor_ids, Competitor)

    # ---- Internal helpers ---- #

    def _get_option(self, entity_id: EntityId, entity_cls: type) -> str:
        """ Label for one entity. Never raises: falls back to the raw ID. """
        try:
            entity = self._client.get(entity_id, entity_cls)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to fetch %s '%s'", entity_cls.__name__, entity_id)
            return str(entity_id)

        if entity is None:
            logger.warning("No %s found for id '%s'", entity_cls.__name__, entity_id)
            return str(entity_id)

        return format_entity_label(entity, entity_id)

    def _get_options(self, entity_ids: list[EntityId], entity_cls: type) -> dict[EntityId, str]:
        if len(entity_ids) > 30:
            logger.warning(
                "Received a large number of %s IDs (%d). This may impact performance.",
                entity_cls.__name__, len(entity_ids),
            )
        return {entity_id: self._get_option(entity_id, entity_cls) for entity_id in entity_ids}
