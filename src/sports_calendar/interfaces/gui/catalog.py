from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol

from sportindex import Competition, Competitor, Sport, SportClient
from sportindex.exceptions import FetchError, NetworkError, RateLimitError

from sports_calendar.core.rounds import MIXED_PHASE_LABEL, rounds_for_competition

if TYPE_CHECKING:
    from sports_calendar.core import EntityId

logger = logging.getLogger(__name__)


class SearchUnavailableError(RuntimeError):
    """ The provider could not be reached at all.

    Distinct from a search that legitimately found nothing. The two look
    identical in the UI — an empty result list — and telling them apart is the
    difference between "no such team" and "you are behind a proxy", which is a
    failure that has cost an evening to diagnose with the source in hand.
    """


# Everything the provider raises when the request never completed. A 404 is
# deliberately not here: that is an answer, not a failure to reach anyone.
_UNREACHABLE = (NetworkError, FetchError, RateLimitError)


# ==== Protocols ====

class FilterSearchProvider(Protocol):
    def search_competition(self, query: str, sport_id: int) -> dict[EntityId, str]: ...

    def search_competitor(self, query: str, sport_id: int) -> dict[EntityId, str]: ...

    def get_competition_option(self, competition_id: EntityId) -> str: ...

    def get_competitor_option(self, competitor_id: EntityId) -> str: ...

    def get_competition_options(self, competition_ids: list[EntityId]) -> dict[EntityId, str]: ...

    def get_competitor_options(self, competitor_ids: list[EntityId]) -> dict[EntityId, str]: ...

    def get_shared_rounds(self, competition_ids: list[EntityId]) -> dict[str, str]: ...


# ==== Formatting Helpers ====

# Athletes are individuals and reading "Carlos Alcaraz (M)" is absurd; teams are
# the ambiguous case, because a club and its women's side share a name exactly.
# The distinction is the entity's own id prefix: `team`/`t-cpt` is a team-shaped
# competitor, `t-ath`/`p-ath`/`p-cpt` is a person.
_TEAM_ID_PREFIXES = ("team", "t-cpt")

_GENDER_SUFFIXES = {"M": "(M)", "F": "(F)"}


def _is_team(entity_id: EntityId) -> bool:
    return str(entity_id).rsplit(":", 1)[0].split(":")[-1] in _TEAM_ID_PREFIXES


def _gender_suffix(entity: object | None, entity_id: EntityId) -> str:
    """ The gender marker for a team, or nothing at all.

    Searching "PSG" or "France" returns the men's and women's sides as two
    identical rows, so picking one is guesswork and there is no way to tell
    afterwards which was chosen. Both are marked rather than only the women's
    side, so neither reads as the default.
    """
    if not _is_team(entity_id):
        return ""

    gender = getattr(entity, "gender", None)
    if gender is None:
        return ""

    return _GENDER_SUFFIXES.get(getattr(gender, "value", gender), "")


def format_entity_label(entity: object | None, fallback: EntityId) -> str:
    """ Human-readable label for an entity, falling back to its raw ID. """
    name = str(getattr(entity, "short_name", None) or getattr(entity, "name", fallback))
    suffix = _gender_suffix(entity, fallback)
    return f"{name} {suffix}" if suffix else name


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


# Failed lookups are deliberately not cached: a name that fell back to its raw
# id because the network blinked should be retried, not remembered.
_LABEL_CACHE: dict[tuple[str, str], str] = {}

# The entities behind those labels. Kept because two different things are wanted
# from the same fetch — a competition's name for the card, and its rounds for
# the selector — and caching only the label meant the rounds lookup re-fetched
# an entity that was already in memory, at about a second a time.
_ENTITY_CACHE: dict[tuple[str, str], object] = {}

# Rounds cost a competition fetch plus a season fetch, and the round selector
# re-reads them every time a competition is added or removed.
_ROUNDS_CACHE: dict[str, dict[str, str]] = {}


# ==== Provider Implementation ====

class SportIndexFilterSearchProvider:
    def __init__(self, client: SportClient):
        self._client = client

    def search_competition(self, query: str, sport_id: int) -> dict[EntityId, str]:
        return self._search(Competition, query, sport_id)

    def search_competitor(self, query: str, sport_id: int) -> dict[EntityId, str]:
        return self._search(Competitor, query, sport_id)

    def _search(self, entity_cls: type, query: str, sport_id: int) -> dict[EntityId, str]:
        """ Matching entities of one sport, as id -> label.

        Raises `SearchUnavailableError` when the provider could not be reached, so
        the caller can say so instead of rendering an empty list that reads as
        "no such team".
        """
        target_sport_id = Sport.encode_id(sport_id)
        try:
            found = self._client.search(entity_cls, query=query, max_results=50)
        except _UNREACHABLE as exc:
            logger.warning("Search for %r could not reach the provider: %s", query, exc)
            raise SearchUnavailableError(str(exc)) from exc

        return {
            entity.id: format_entity_label(entity, entity.id)
            for entity in found
            if getattr(getattr(entity, "sport", None), "id", target_sport_id) == target_sport_id
        }

    def get_competition_option(self, competition_id: EntityId) -> str:
        return self._get_option(competition_id, Competition)

    def get_competitor_option(self, competitor_id: EntityId) -> str:
        return self._get_option(competitor_id, Competitor)

    def get_competition_options(self, competition_ids: list[EntityId]) -> dict[EntityId, str]:
        return self._get_options(competition_ids, Competition)

    def get_competitor_options(self, competitor_ids: list[EntityId]) -> dict[EntityId, str]:
        return self._get_options(competitor_ids, Competitor)

    def get_shared_rounds(self, competition_ids: list[EntityId]) -> dict[str, str]:
        """ Rounds every one of these competitions has, as slug -> display name.

        Only rounds shared by all of them are offered: a round that exists in
        one competition and not another would quietly match nothing for the
        rest. Grouping competitions is how someone follows "the final of the
        Europa League, the Conference League and the FA Cup", and those do share
        their late rounds even when their early ones differ completely.

        Empty when nothing is shared — which is the answer for a league grouped
        with a cup, and the UI says so rather than showing an empty menu.
        """
        if not competition_ids:
            return {}

        shared: dict[str, str] | None = None
        for competition_id in competition_ids:
            rounds = self._get_named_rounds(competition_id)
            if shared is None:
                shared = dict(rounds)
            else:
                # A slug kept by all of them, but the phase entry can be called
                # "Group phase" in one and "League phase" in another — grouping
                # the Champions League with Ligue 1 does exactly that. It is the
                # same set of matches either way, so it stays on offer under a
                # label that claims neither.
                shared = {
                    slug: (name if rounds[slug] == name else MIXED_PHASE_LABEL)
                    for slug, name in shared.items()
                    if slug in rounds
                }
            if not shared:
                return {}
        return shared or {}

    def _get_named_rounds(self, competition_id: EntityId) -> dict[str, str]:
        """ A competition's selectable rounds, slug -> label, in provider order.

        Read from the previous season, because the current one only lists what
        has been drawn so far — the Champions League publishes four rounds in
        August and seventeen by spring.

        See `core.rounds` for which rounds are worth offering: the knockout
        rounds, plus a single "Group phase" entry when the competition has one.
        """
        cached = _ROUNDS_CACHE.get(str(competition_id))
        if cached is not None:
            return cached

        competition = self._get_entity(competition_id, Competition)
        try:
            selectable = rounds_for_competition(competition) if competition else {}
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to read rounds for competition '%s'", competition_id)
            return {}

        _ROUNDS_CACHE[str(competition_id)] = selectable
        return selectable

    # ---- Internal helpers ---- #

    def _get_entity(self, entity_id: EntityId, entity_cls: type) -> object | None:
        """ One entity, fetched at most once per process. Never raises.

        Cached for the life of the process because nothing here changes while
        the app is open, and because the same entity is wanted for two unrelated
        reasons — its name, and (for a competition) its rounds.
        """
        cache_key = (entity_cls.__name__, str(entity_id))
        if cache_key in _ENTITY_CACHE:
            return _ENTITY_CACHE[cache_key]

        try:
            entity = self._client.get(entity_id, entity_cls)
        except Exception:  # noqa: BLE001 - UI boundary, a lookup must not break the page
            logger.exception("Failed to fetch %s '%s'", entity_cls.__name__, entity_id)
            return None

        if entity is None:
            logger.warning("No %s found for id '%s'", entity_cls.__name__, entity_id)
            return None

        _ENTITY_CACHE[cache_key] = entity
        return entity

    def _get_option(self, entity_id: EntityId, entity_cls: type) -> str:
        """ Label for one entity, falling back to the raw ID.

        Every filter card renders the names of what it contains, so without a
        cache, opening a page with a handful of rules costs a round-trip per
        team on every render.
        """
        cache_key = (entity_cls.__name__, str(entity_id))
        cached = _LABEL_CACHE.get(cache_key)
        if cached is not None:
            return cached

        entity = self._get_entity(entity_id, entity_cls)
        if entity is None:
            # Not cached: a name that fell back to its raw id because the
            # network blinked should be retried, not remembered.
            return str(entity_id)

        label = format_entity_label(entity, entity_id)
        _LABEL_CACHE[cache_key] = label
        return label

    def _get_options(self, entity_ids: list[EntityId], entity_cls: type) -> dict[EntityId, str]:
        if len(entity_ids) > 30:
            logger.warning(
                "Received a large number of %s IDs (%d). This may impact performance.",
                entity_cls.__name__, len(entity_ids),
            )
        return {entity_id: self._get_option(entity_id, entity_cls) for entity_id in entity_ids}
