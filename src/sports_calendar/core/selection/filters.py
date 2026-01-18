from __future__ import annotations

from abc import ABC
from uuid import uuid4
from typing import ClassVar
from dataclasses import dataclass, field

from . import logger
from ..competition_stages import CompetitionStage
from ..utils import validate


# Base class for selection filters

@dataclass(frozen=True)
class SelectionFilter(ABC):
    sport: str
    uid: str = field(default_factory=lambda: str(uuid4())[:8], kw_only=True)
    filter_type: ClassVar[str]

    def __post_init__(self):
        validate(bool(self.sport), "SelectionFilter.sport cannot be empty", logger)
        validate(isinstance(self.filter_type, str), "SelectionFilter.filter_type must be a string", logger, TypeError)

    def to_dict(self) -> dict:
        data = {}
        for field_name in self.__dataclass_fields__:
            if field_name != "sport":
                data[field_name] = getattr(self, field_name)
        return data

    def clone(self) -> SelectionFilter:
        """ Create a deep copy of this SelectionFilter with a new ID. """
        return type(self)(**{f: getattr(self, f) for f in self.__dataclass_fields__ if f not in ("uid", "filter_type")})

    @classmethod
    def from_dict(cls, sport: str, data: dict) -> SelectionFilter:
        """ Create a SelectionFilter from a dictionary. """
        data = dict(data) 
        filter_type = data.pop("filter_type", None)
        validate(filter_type is not None, "filter_type is required to create SelectionFilter", logger, KeyError)
        for subclass in cls.__subclasses__():
            if getattr(subclass, "filter_type", None) == filter_type:
                return subclass(sport=sport, **data)
        logger.error(f"Unknown filter type: {filter_type}")
        raise KeyError(f"Unknown filter type: {filter_type}")

    @classmethod
    def empty(cls, sport: str, filter_type: str) -> SelectionFilter:
        """ Create an empty SelectionFilter for the given sport and filter type. """
        for subclass in cls.__subclasses__():
            if getattr(subclass, "filter_type", None) == filter_type:
                return subclass(sport=sport)
        logger.error(f"Unknown filter type: {filter_type}")
        raise KeyError(f"Unknown filter type: {filter_type}")


# Specific filter implementations

@dataclass(frozen=True)
class MinRankingFilter(SelectionFilter):
    rule: str                     # "both" | "any" | "opponent"
    ranking: int
    competition_ids: list[int] = field(default_factory=list)
    reference_team: str | None = None

    filter_type: ClassVar[str] = "min_ranking"

    def __post_init__(self):
        super().__post_init__()

        valid_rules = self.valid_rules()
        validate(
            self.rule in valid_rules,
            f"Invalid rule '{self.rule}'. Must be one of {sorted(valid_rules)}",
            logger
        )
        validate(
            isinstance(self.ranking, int) and self.ranking > 0,
            "ranking must be a strictly positive integer",
            logger
        )
        validate(
            self.rule != "opponent" or self.reference_team is not None,
            "reference_team must be provided when rule == 'opponent'",
            logger
        )
        validate(isinstance(self.competition_ids, list), "competition_ids must be a list", logger, TypeError)
        for cid in self.competition_ids:
            validate(isinstance(cid, int), "competition_ids must contain only integers", logger, TypeError)

    def valid_rules(self) -> set[str]:
        return {"both", "any", "opponent"}


@dataclass(frozen=True)
class StageFilter(SelectionFilter):
    stage: CompetitionStage
    competition_ids: list[int] = field(default_factory=list)

    filter_type: ClassVar[str] = "stage"

    def __post_init__(self):
        if isinstance(self.stage, str):
            object.__setattr__(self, "stage", CompetitionStage.from_str(self.stage))

        super().__post_init__()

        validate(
            isinstance(self.stage, CompetitionStage),
            "stage must be a CompetitionStage instance",
            logger,
            TypeError
        )
        validate(isinstance(self.competition_ids, list), "competition_ids must be a list", logger, TypeError)
        for cid in self.competition_ids:
            validate(isinstance(cid, int), "competition_ids must contain only integers", logger, TypeError)


@dataclass(frozen=True)
class TeamsFilter(SelectionFilter):
    team_ids: list[int]
    rule: str                     # "both" | "any"

    filter_type: ClassVar[str] = "teams"

    def __post_init__(self):
        super().__post_init__()

        valid_rules = self.valid_rules()
        validate(
            self.rule in valid_rules,
            f"Invalid rule '{self.rule}'. Must be one of {sorted(valid_rules)}",
            logger
        )
        validate(bool(self.team_ids), "team_ids cannot be empty", logger)
        validate(isinstance(self.team_ids, list), "team_ids must be a list", logger, TypeError)
        for tid in self.team_ids:
            validate(isinstance(tid, int), "team_ids must contain only integers", logger, TypeError)

    def valid_rules(self) -> set[str]:
        return {"both", "any"}


@dataclass(frozen=True)
class CompetitionsFilter(SelectionFilter):
    competition_ids: list[int]
    filter_type: ClassVar[str] = "competitions"

    def __post_init__(self):
        super().__post_init__()

        validate(bool(self.competition_ids), "competition_ids cannot be empty", logger)
        validate(isinstance(self.competition_ids, list), "competition_ids must be a list", logger, TypeError)
        for cid in self.competition_ids:
            validate(isinstance(cid, int), "competition_ids must contain only integers", logger, TypeError)


@dataclass(frozen=True)
class SessionFilter(SelectionFilter):
    sessions: list[str]
    filter_type: ClassVar[str] = "session"

    def __post_init__(self):
        super().__post_init__()

        validate(bool(self.sessions), "sessions cannot be empty", logger)
        validate(isinstance(self.sessions, list), "sessions must be a list", logger, TypeError)
        for s in self.sessions:
            validate(
                isinstance(s, str) and bool(s),
                "sessions must contain non-empty strings",
                logger,
                TypeError
            )
