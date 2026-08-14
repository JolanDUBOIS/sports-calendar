from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, TypeAlias

from sportindex import StageTier

from sports_calendar.core import EntityId
from sports_calendar.core.utils import validate

from .enums import FilterType

logger = logging.getLogger(__name__)


# ==== Entity Selection Rule ====

class Rule(Enum):
    ANY = "any"
    BOTH = "both"
    OPPONENT = "opponent"


@dataclass
class EntitySelectionRule:
    rule: Rule = Rule.ANY
    reference: EntityId | None = None  # only for OPPONENT

    def __post_init__(self):
        validate(isinstance(self.rule, Rule),
                 "rule must be an instance of Rule Enum", logger)
        if self.rule == Rule.OPPONENT:
            validate(self.reference is not None,
                 "reference is required when rule is OPPONENT", logger)

    def to_dict(self) -> dict:
        return {
            "rule": self.rule.value,
            "reference": self.reference
        }

    @classmethod
    def from_dict(cls, data: dict) -> EntitySelectionRule:
        return cls(
            rule=Rule(data.get("rule", Rule.ANY.value)),
            reference=data.get("reference")
        )

# ==== Filter Fields Data Classes ====

@dataclass
class EmptyFilterFields:
    filter_type: Literal[FilterType.EMPTY] = FilterType.EMPTY

    def clone(self) -> EmptyFilterFields:
        return EmptyFilterFields()

    def to_dict(self) -> dict:
        return {
            "filter_type": self.filter_type.value
        }

    @classmethod
    def from_dict(cls, data: dict) -> EmptyFilterFields:
        if not data.get("filter_type") == FilterType.EMPTY.value:
            raise ValueError(f"Invalid filter_type for EmptyFilterFields: {data.get('filter_type')}")
        return cls()

@dataclass
class MinRankingFilterFields:
    ranking: int
    competition_ids: list[EntityId]
    filter_type: Literal[FilterType.MIN_RANKING] = FilterType.MIN_RANKING
    selection_rule: EntitySelectionRule = field(default_factory=EntitySelectionRule)

    def __post_init__(self):
        validate(isinstance(self.ranking, int) and self.ranking > 0,
                 "ranking must be a positive integer", logger)
        validate(isinstance(self.competition_ids, list) and all(isinstance(cid, EntityId) for cid in self.competition_ids),
                 "competition_ids must be a list of EntityId", logger)

    def clone(self) -> MinRankingFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "ranking": self.ranking,
            "competition_ids": self.competition_ids,
            "filter_type": self.filter_type.value,
            "selection_rule": self.selection_rule.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict) -> MinRankingFilterFields:
        if not data.get("filter_type") == FilterType.MIN_RANKING.value:
            raise ValueError(f"Invalid filter_type for MinRankingFilterFields: {data.get('filter_type')}")
        return cls(
            ranking=data["ranking"],
            competition_ids=data["competition_ids"],
            selection_rule=EntitySelectionRule.from_dict(data.get("selection_rule", {}))
        )

@dataclass
class WorldRankingFilterFields:
    """ Follow whoever sits near the top of a governing body's ranking.

    `sport_id` is carried here, redundantly with the owning SelectionFilter,
    because rankings are not addressable entities in sport-index: they are only
    reachable through `Sport.get_rankings()`, so the executor needs to know the
    sport to find the table at all.
    """
    ranking: int
    ranking_id: int
    sport_id: int
    filter_type: Literal[FilterType.WORLD_RANKING] = FilterType.WORLD_RANKING
    selection_rule: EntitySelectionRule = field(default_factory=EntitySelectionRule)

    def __post_init__(self):
        validate(isinstance(self.ranking, int) and self.ranking > 0,
                 "ranking must be a positive integer", logger)
        validate(isinstance(self.ranking_id, int),
                 "ranking_id must be an integer", logger)
        validate(isinstance(self.sport_id, int),
                 "sport_id must be an integer", logger)

    def clone(self) -> WorldRankingFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "ranking": self.ranking,
            "ranking_id": self.ranking_id,
            "sport_id": self.sport_id,
            "filter_type": self.filter_type.value,
            "selection_rule": self.selection_rule.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict) -> WorldRankingFilterFields:
        if not data.get("filter_type") == FilterType.WORLD_RANKING.value:
            raise ValueError(f"Invalid filter_type for WorldRankingFilterFields: {data.get('filter_type')}")
        return cls(
            ranking=data["ranking"],
            ranking_id=data["ranking_id"],
            sport_id=data["sport_id"],
            selection_rule=EntitySelectionRule.from_dict(data.get("selection_rule", {}))
        )

@dataclass
class CompetitionsFilterFields:
    """ Whole competitions, optionally narrowed to particular rounds.

    `from_round` is a sport-index round *slug* ("quarterfinals"), and means
    "this round and everything after it". None means the whole competition.

    A slug rather than an ordering of our own: each competition reports its
    rounds in the order they are played, so "from the quarter-finals" is
    resolved against that competition's own ladder. Competitions also
    reorganise — the Champions League gained a league phase in 24/25 — and
    re-reading the provider's rounds survives that, where a hand-maintained
    taxonomy would not.
    """
    competition_ids: list[EntityId]
    filter_type: Literal[FilterType.COMPETITIONS] = FilterType.COMPETITIONS
    from_round: str | None = None

    def __post_init__(self):
        validate(self.from_round is None or isinstance(self.from_round, str),
                 "from_round must be a round slug or None", logger)
        validate(isinstance(self.competition_ids, list) and all(isinstance(cid, EntityId) for cid in self.competition_ids),
                 "competition_ids must be a list of EntityId", logger)

    def clone(self) -> CompetitionsFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "competition_ids": self.competition_ids,
            "filter_type": self.filter_type.value,
            "from_round": self.from_round
        }

    @classmethod
    def from_dict(cls, data: dict) -> CompetitionsFilterFields:
        if not data.get("filter_type") == FilterType.COMPETITIONS.value:
            raise ValueError(f"Invalid filter_type for CompetitionsFilterFields: {data.get('filter_type')}")
        # Filters saved before this carry a `stage` key that was never populated
        # or read; it is simply ignored.
        return cls(
            competition_ids=data["competition_ids"],
            from_round=data.get("from_round")
        )

@dataclass
class CompetitorsFilterFields:
    competitor_ids: list[EntityId]
    filter_type: Literal[FilterType.COMPETITORS] = FilterType.COMPETITORS
    selection_rule: EntitySelectionRule = field(default_factory=EntitySelectionRule)

    def __post_init__(self):
        validate(isinstance(self.competitor_ids, list) and all(isinstance(cid, EntityId) for cid in self.competitor_ids),
                 "competitor_ids must be a list of EntityId", logger)

    def clone(self) -> CompetitorsFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "competitor_ids": self.competitor_ids,
            "filter_type": self.filter_type.value,
            "selection_rule": self.selection_rule.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict) -> CompetitorsFilterFields:
        if not data.get("filter_type") == FilterType.COMPETITORS.value:
            raise ValueError(f"Invalid filter_type for CompetitorsFilterFields: {data.get('filter_type')}")
        return cls(
            competitor_ids=data["competitor_ids"],
            selection_rule=EntitySelectionRule.from_dict(data.get("selection_rule", {}))
        )

@dataclass
class SessionsFilterFields:
    competition_id: EntityId
    sessions: list[StageTier]
    filter_type: Literal[FilterType.SESSIONS] = FilterType.SESSIONS

    def __post_init__(self):
        validate(isinstance(self.competition_id, EntityId), "competition_id must be an EntityId", logger)
        validate(isinstance(self.sessions, list) and all(isinstance(session, StageTier) for session in self.sessions),
                 "sessions must be a list of StageTier", logger)

    def clone(self) -> SessionsFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "competition_id": self.competition_id,
            "sessions": [session.value for session in self.sessions],
            "filter_type": self.filter_type.value
        }

    @classmethod
    def from_dict(cls, data: dict) -> SessionsFilterFields:
        if not data.get("filter_type") == FilterType.SESSIONS.value:
            raise ValueError(f"Invalid filter_type for SessionsFilterFields: {data.get('filter_type')}")
        return cls(
            competition_id=data["competition_id"],
            sessions=[StageTier(value) for value in data["sessions"]]
        )


# ==== Filter Fields Protocol ====

FilterFields: TypeAlias = (
    EmptyFilterFields
    | MinRankingFilterFields
    | WorldRankingFilterFields
    | CompetitionsFilterFields
    | CompetitorsFilterFields
    | SessionsFilterFields
)

# NOTE: Teams, Competitions, MinRanking are coded as uniquely designed for opposition sports (football, basketball, tennis, etc.)
# Sessions is coded as uniquely designed for race sports (motorsports, cycling)

# TODO: Switch from TeamsFilterFields to CompetitorsFilterFields with a competitor type (team or individual) since the client provides a unified interface for both teams and players.
