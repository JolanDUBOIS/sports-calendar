from __future__ import annotations

from abc import ABC
from uuid import uuid4
from dataclasses import dataclass, field

from . import logger
from ..competition_stages import CompetitionStage
from ..utils import validate


# =========================
# Selection specs
# =========================

@dataclass
class SelectionSpec:
    name: str
    items: list[SelectionItemSpec] = field(default_factory=list)
    uid: str = field(default_factory=lambda: str(uuid4()))

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

    @property
    def sports(self) -> set[str]:
        return {item.sport for item in self.items}

    def get_item(self, item_uid: str) -> SelectionItemSpec:
        for item in self.items:
            if item.uid == item_uid:
                return item
        logger.error(f"Item with uid '{item_uid}' not found in selection '{self.uid}'.")
        raise KeyError(f"Item with uid '{item_uid}' not found in selection '{self.uid}'.")

    def add_item(self, item: SelectionItemSpec):
        validate(
            isinstance(item, SelectionItemSpec),
            "item must be an instance of SelectionItemSpec",
            logger,
            TypeError
        )
        self.items.append(item)
        logger.info(f"Added item {item.uid} to selection {self.uid}")

    def remove_item(self, item_uid: str):
        self.items = [item for item in self.items if item.uid != item_uid]
        logger.info(f"Removed item {item_uid} from selection {self.uid}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "uid": self.uid,
            "items": [item.to_dict() for item in self.items]
        }

    def clone(self, new_name: str) -> SelectionSpec:
        """ Create a deep copy of this SelectionSpec with new IDs. """
        cloned_items = [item.clone() for item in self.items]
        return SelectionSpec(
            name=new_name,
            items=cloned_items
        )

    @classmethod
    def from_dict(cls, data: dict) -> SelectionSpec:
        """ Create a SelectionSpec from a dictionary. """
        data = dict(data)
        items_data = data.pop("items", [])
        items = [SelectionItemSpec.from_dict(item_data) for item_data in items_data]
        return cls(**data, items=items)

    @classmethod
    def empty(cls, name: str) -> SelectionSpec:
        """ Create an empty SelectionSpec with the given name. """
        return cls(name=name, items=[])


@dataclass
class SelectionItemSpec:
    sport: str
    filters: list[SelectionFilterSpec] = field(default_factory=list)
    uid: str = field(default_factory=lambda: str(uuid4()))

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

    def get_filter(self, filter_uid: str) -> SelectionFilterSpec:
        for f in self.filters:
            if f.uid == filter_uid:
                return f
        logger.error(f"Filter with uid '{filter_uid}' not found in item '{self.uid}'.")
        raise KeyError(f"Filter with uid '{filter_uid}' not found in item '{self.uid}'.")

    def add_filter(self, filter: SelectionFilterSpec):
        validate(
            isinstance(filter, SelectionFilterSpec),
            "filter must be an instance of SelectionFilterSpec",
            logger,
            TypeError
        )
        self.filters.append(filter)
        logger.info(f"Added filter {filter.uid} to item {self.uid}")

    def remove_filter(self, filter_uid: str):
        self.filters = [f for f in self.filters if f.uid != filter_uid]
        logger.info(f"Removed filter {filter_uid} from item {self.uid}")

    def to_dict(self) -> dict:
        return {
            "sport": self.sport,
            "uid": self.uid,
            "filters": [f.to_dict() for f in self.filters]
        }

    def clone(self) -> SelectionItemSpec:
        """ Create a deep copy of this SelectionItemSpec with new IDs. """
        cloned_filters = [f.clone() for f in self.filters]
        return SelectionItemSpec(
            sport=self.sport,
            filters=cloned_filters
        )

    @classmethod
    def from_dict(cls, data: dict) -> SelectionItemSpec:
        """ Create a SelectionItemSpec from a dictionary. """
        data = dict(data) 
        filters_data = data.pop("filters", [])
        filters = [SelectionFilterSpec.from_dict(sport=data.get("sport"), data=f_data) for f_data in filters_data]
        return cls(**data, filters=filters)

    @classmethod
    def empty(cls, sport: str) -> SelectionItemSpec:
        """ Create an empty SelectionItemSpec for the given sport. """
        return cls(sport=sport, filters=[])


# =========================
# Filter specs
# =========================

@dataclass(frozen=True)
class SelectionFilterSpec(ABC):
    sport: str
    uid: str = field(default_factory=lambda: str(uuid4()), kw_only=True)
    filter_type: str = field(init=False)

    def __post_init__(self):
        validate(bool(self.sport), "SelectionFilterSpec.sport cannot be empty", logger)
        validate(isinstance(self.filter_type, str), "SelectionFilterSpec.filter_type must be a string", logger, TypeError)

    def to_dict(self) -> dict:
        data = {}
        for field_name in self.__dataclass_fields__:
            if field_name != "sport":
                data[field_name] = getattr(self, field_name)
        return data

    def clone(self) -> SelectionFilterSpec:
        """ Create a deep copy of this SelectionFilterSpec with a new ID. """
        data = self.to_dict()
        data.pop("uid", None)  # Remove existing ID to generate a new one
        data.pop("filter_type", None)  # filter_type is set in __post_init__
        return type(self)(**data)

    @classmethod
    def from_dict(cls, sport: str, data: dict) -> SelectionFilterSpec:
        """ Create a SelectionFilterSpec from a dictionary. """
        data = dict(data) 
        filter_type = data.pop("filter_type", None)
        validate(filter_type is not None, "filter_type is required to create SelectionFilterSpec", logger, KeyError)
        for subclass in cls.__subclasses__():
            if getattr(subclass, "filter_type", None) == filter_type:
                return subclass(sport=sport, **data)

    @classmethod
    def empty(cls, sport: str, filter_type: str) -> SelectionFilterSpec:
        """ Create an empty SelectionFilterSpec for the given sport and filter type. """
        for subclass in cls.__subclasses__():
            if getattr(subclass, "filter_type", None) == filter_type:
                return subclass(sport=sport)
        logger.error(f"Unknown filter type: {filter_type}")
        raise KeyError(f"Unknown filter type: {filter_type}")


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

    filter_type: str = field(init=False, default="min_ranking", repr=False)

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

    filter_type: str = field(init=False, default="stage", repr=False)

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

    def to_dict(self) -> dict:
        data = super().to_dict()
        data["stage"] = {"name": self.stage.name, "value": self.stage.value}
        return data


@dataclass(frozen=True)
class TeamsFilterSpec(SelectionFilterSpec):
    team_ids: list[int]
    rule: str                     # "both" | "any"

    filter_type: str = field(init=False, default="teams", repr=False)

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
    filter_type: str = field(init=False, default="competitions", repr=False)

    def __post_init__(self):
        super().__post_init__()

        validate(bool(self.competition_ids), "competition_ids cannot be empty", logger)
        validate(isinstance(self.competition_ids, list), "competition_ids must be a list", logger, TypeError)
        for cid in self.competition_ids:
            validate(isinstance(cid, int), "competition_ids must contain only integers", logger, TypeError)


@dataclass(frozen=True)
class SessionFilterSpec(SelectionFilterSpec):
    sessions: list[str]
    filter_type: str = field(init=False, default="session", repr=False)

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
