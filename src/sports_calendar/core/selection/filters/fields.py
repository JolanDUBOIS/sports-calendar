from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, TypeAlias

from sportindex import StageTier

from sports_calendar.core import CompetitionStage, EntityId
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
class CompetitionsFilterFields:
    competition_ids: list[EntityId]
    filter_type: Literal[FilterType.COMPETITIONS] = FilterType.COMPETITIONS
    stage: CompetitionStage = CompetitionStage.NULL

    def __post_init__(self):
        validate(isinstance(self.stage, CompetitionStage),
                 "stage must be a CompetitionStage", logger)
        validate(isinstance(self.competition_ids, list) and all(isinstance(cid, EntityId) for cid in self.competition_ids),
                 "competition_ids must be a list of EntityId", logger)

    def clone(self) -> CompetitionsFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "competition_ids": self.competition_ids,
            "filter_type": self.filter_type.value,
            "stage": self.stage.value
        }

    @classmethod
    def from_dict(cls, data: dict) -> CompetitionsFilterFields:
        if not data.get("filter_type") == FilterType.COMPETITIONS.value:
            raise ValueError(f"Invalid filter_type for CompetitionsFilterFields: {data.get('filter_type')}")
        return cls(
            competition_ids=data["competition_ids"],
            stage=CompetitionStage(data.get("stage", CompetitionStage.NULL.value))
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
    | CompetitionsFilterFields
    | CompetitorsFilterFields
    | SessionsFilterFields
)

# NOTE: Teams, Competitions, MinRanking are coded as uniquely designed for opposition sports (football, basketball, tennis, etc.)
# Sessions is coded as uniquely designed for race sports (motorsports, cycling)

# TODO: Switch from TeamsFilterFields to CompetitorsFilterFields with a competitor type (team or individual) since the client provides a unified interface for both teams and players.
