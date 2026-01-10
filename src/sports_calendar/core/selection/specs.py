from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field

from . import logger
from ..competition_stages import CompetitionStage
from ..utils import validate


# =========================
# Selection specs
# =========================

class SelectionSpec:
    name: str
    items: list[SelectionItemSpec] = field(default_factory=list)

    def __post_init__(self):
        validate(bool(self.name), "SelectionSpec.name cannot be empty", logger)
        validate(
            isinstance(self.items, list),
            "items must be a list of SelectionItemSpec",
            logger,
            TypeError
        )
        for item in self.items:
            validate(
                isinstance(item, SelectionItemSpec),
                "All items must be instances of SelectionItemSpec",
                logger,
                TypeError
            )

    def add_item(self, item: SelectionItemSpec):
        validate(
            isinstance(item, SelectionItemSpec),
            "item must be an instance of SelectionItemSpec",
            logger,
            TypeError
        )
        self.items.append(item)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "items": [item.to_dict() for item in self.items]
        }


class SelectionItemSpec:
    sport: str
    filters: list[SelectionFilterSpec] = field(default_factory=list)

    def __post_init__(self):
        validate(
            bool(self.sport),
            "SelectionItemSpec.sport cannot be empty",
            logger
        )
        validate(
            isinstance(self.filters, list),
            "filters must be a list of SelectionFilterSpec",
            logger,
            TypeError
        )
        for f in self.filters:
            validate(isinstance(f, SelectionFilterSpec), 
                "All filters must be instances of SelectionFilterSpec",
                logger,
                TypeError
            )

    def add_filter(self, filter: SelectionFilterSpec):
        validate(
            isinstance(filter, SelectionFilterSpec),
            "filter must be an instance of SelectionFilterSpec",
            logger,
            TypeError
        )
        self.filters.append(filter)

    def to_dict(self) -> dict:
        return {
            "sport": self.sport,
            "filters": [f.to_dict() for f in self.filters]
        }


# =========================
# Filter specs
# =========================

@dataclass(frozen=True)
class SelectionFilterSpec(ABC):
    sport: str
    key: str = field(init=False)

    def __post_init__(self):
        validate(bool(self.sport), "SelectionFilterSpec.sport cannot be empty", logger)
        validate(isinstance(self.key, str), "SelectionFilterSpec.key must be a string", logger, TypeError)

    def to_dict(self) -> dict:
        data = {"key": self.key, "sport": self.sport}
        for field_name in self.__dataclass_fields__:
            if field_name not in {"key", "sport"}:
                data[field_name] = getattr(self, field_name)
        return data


# -------------------------
# Concrete filter specs
# -------------------------

# WARNING - The types "list" prevents perfect immutability, but is necessary for usability.

@dataclass(frozen=True)
class MinRankingFilterSpec(SelectionFilterSpec):
    rule: str                     # "both" | "any" | "opponent"
    ranking: int
    competition_ids: list[int] = field(default_factory=list)
    reference_team: str | None = None

    key: str = field(init=False, default="min_ranking", repr=False)

    def __post_init__(self):
        super().__post_init__()

        valid_rules = {"both", "any", "opponent"}
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


@dataclass(frozen=True)
class StageFilterSpec(SelectionFilterSpec):
    stage: CompetitionStage

    key: str = field(init=False, default="stage", repr=False)

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


@dataclass(frozen=True)
class TeamsFilterSpec(SelectionFilterSpec):
    team_ids: list[int]
    rule: str                     # "both" | "any"

    key: str = field(init=False, default="teams", repr=False)

    def __post_init__(self):
        super().__post_init__()

        valid_rules = {"both", "any"}
        validate(
            self.rule in valid_rules,
            f"Invalid rule '{self.rule}'. Must be one of {sorted(valid_rules)}",
            logger
        )
        validate(bool(self.team_ids), "team_ids cannot be empty", logger)
        validate(isinstance(self.team_ids, list), "team_ids must be a list", logger, TypeError)
        for tid in self.team_ids:
            validate(isinstance(tid, int), "team_ids must contain only integers", logger, TypeError)


@dataclass(frozen=True)
class CompetitionsFilterSpec(SelectionFilterSpec):
    competition_ids: list[int]
    key: str = field(init=False, default="competitions", repr=False)

    def __post_init__(self):
        super().__post_init__()

        validate(bool(self.competition_ids), "competition_ids cannot be empty", logger)
        validate(isinstance(self.competition_ids, list), "competition_ids must be a list", logger, TypeError)
        for cid in self.competition_ids:
            validate(isinstance(cid, int), "competition_ids must contain only integers", logger, TypeError)


@dataclass(frozen=True)
class SessionFilterSpec(SelectionFilterSpec):
    sessions: list[str]
    key: str = field(init=False, default="session", repr=False)

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
