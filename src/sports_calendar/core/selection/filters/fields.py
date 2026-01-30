from __future__ import annotations
import copy
from enum import Enum
from typing import Protocol
from dataclasses import dataclass, field

from . import logger
from sports_calendar.core import CompetitionStage
from sports_calendar.core.utils import validate


# ==== Entity Selection Rule ====

class Rule(Enum):
    BOTH = "both"
    ANY = "any"
    OPPONENT = "opponent"

@dataclass
class EntitySelectionRule:
    rule: Rule = Rule.BOTH
    reference: str | None = None  # only for OPPONENT

    def __post_init__(self):
        validate(isinstance(self.rule, Rule),
                 f"rule must be an instance of Rule Enum", logger)
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
            rule=Rule(data.get("rule", Rule.BOTH.value)),
            reference=data.get("reference")
        )

# ==== Filter Fields Data Classes ====

@dataclass
class EmptyFilterFields:
    def clone(self) -> EmptyFilterFields:
        return EmptyFilterFields()

    def to_dict(self) -> dict:
        return {}

    @classmethod
    def from_dict(cls, data: dict) -> EmptyFilterFields:
        return cls()

@dataclass
class MinRankingFilterFields:
    ranking: int
    competition_ids: list[int]
    selection_rule: EntitySelectionRule = field(default_factory=EntitySelectionRule)

    def __post_init__(self):
        validate(isinstance(self.ranking, int) and self.ranking > 0,
                 "ranking must be a positive integer", logger)
        validate(isinstance(self.competition_ids, list) and all(isinstance(cid, int) for cid in self.competition_ids),
                 "competition_ids must be a list of integers", logger)

    def clone(self) -> MinRankingFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "ranking": self.ranking,
            "competition_ids": self.competition_ids,
            "selection_rule": self.selection_rule.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict) -> MinRankingFilterFields:
        return cls(
            ranking=data["ranking"],
            competition_ids=data["competition_ids"],
            selection_rule=EntitySelectionRule.from_dict(data.get("selection_rule", {}))
        )
        
@dataclass
class CompetitionFilterFields:
    competition_ids: list[int]
    stage: CompetitionStage = CompetitionStage.NULL

    def __post_init__(self):
        validate(isinstance(self.stage, CompetitionStage),
                 "stage must be a CompetitionStage", logger)
        validate(isinstance(self.competition_ids, list) and all(isinstance(cid, int) for cid in self.competition_ids),
                 "competition_ids must be a list of integers", logger)

    def clone(self) -> CompetitionFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "competition_ids": self.competition_ids,
            "stage": self.stage.value
        }

    @classmethod
    def from_dict(cls, data: dict) -> CompetitionFilterFields:
        return cls(
            competition_ids=data["competition_ids"],
            stage=CompetitionStage(data.get("stage", CompetitionStage.NULL.value))
        )

@dataclass
class TeamsFilterFields:
    team_ids: list[int]
    selection_rule: EntitySelectionRule = field(default_factory=EntitySelectionRule)

    def __post_init__(self):
        validate(isinstance(self.team_ids, list) and all(isinstance(tid, int) for tid in self.team_ids),
                 "team_ids must be a list of integers", logger)

    def clone(self) -> TeamsFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "team_ids": self.team_ids,
            "selection_rule": self.selection_rule.to_dict()
        }

    @classmethod
    def from_dict(cls, data: dict) -> TeamsFilterFields:
        return cls(
            team_ids=data["team_ids"],
            selection_rule=EntitySelectionRule.from_dict(data.get("selection_rule", {}))
        )

@dataclass
class SessionFilterFields:
    sessions: list[str]  # e.g., ["Race", "Sprint", "FP1"]
    # Note: sessions might evolve when more sports are added, we might need to create enums...

    def __post_init__(self):
        validate(isinstance(self.sessions, list) and all(isinstance(session, str) for session in self.sessions),
                 "sessions must be a list of strings", logger)

    def clone(self) -> SessionFilterFields:
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {
            "sessions": self.sessions
        }

    @classmethod
    def from_dict(cls, data: dict) -> SessionFilterFields:
        return cls(
            sessions=data["sessions"]
        )


# ==== Filter Fields Protocol ====

class FilterFields(Protocol):
    def clone(self) -> FilterFields: ...
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, data: dict) -> FilterFields: ...
