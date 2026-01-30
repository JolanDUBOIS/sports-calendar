from __future__ import annotations
from uuid import uuid4
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field

from . import logger
from .fields import (
    FilterFields,
    EmptyFilterFields,
    MinRankingFilterFields,
    CompetitionFilterFields,
    TeamsFilterFields,
    SessionFilterFields
)
from sports_calendar.core.utils import validate, validate_timestamp
from sports_calendar.core.db import SPORT_SCHEMAS


# === Filter Type Enum ====

class FilterType(Enum):
    EMPTY = "empty"
    MIN_RANKING = "min_ranking"
    COMPETITIONS = "competitions"
    TEAMS = "teams"
    SESSION = "session"


FILTER_TYPE_TO_FIELDS: dict[FilterType, type[FilterFields]] = {
    FilterType.EMPTY: EmptyFilterFields,
    FilterType.MIN_RANKING: MinRankingFilterFields,
    FilterType.COMPETITIONS: CompetitionFilterFields,
    FilterType.TEAMS: TeamsFilterFields,
    FilterType.SESSION: SessionFilterFields,
}


# === Selection Filter Data Class ====

@dataclass(frozen=True)
class SelectionFilter:
    sport: str
    name: str = ""
    uid: str = field(default_factory=lambda: str(uuid4())[:8])
    filter_type: FilterType = FilterType.EMPTY
    fields: FilterFields = field(default_factory=EmptyFilterFields)
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    updated_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def __post_init__(self):
        validate(self.sport in SPORT_SCHEMAS, f"Invalid sport: {self.sport}", logger)
        expected_cls = FILTER_TYPE_TO_FIELDS.get(self.filter_type)
        validate(isinstance(self.fields, expected_cls),
                 f"fields must be of type {expected_cls.__name__} for filter_type {self.filter_type.value}", logger)
        validate_timestamp(self.created_at, "created_at", logger)
        validate_timestamp(self.updated_at, "updated_at", logger)

    def clone(self) -> SelectionFilter:
        """ Create a deep copy of this SelectionFilter with a new ID. """
        return SelectionFilter(
            sport=self.sport,
            name=self.name,
            filter_type=self.filter_type,
            fields=self.fields.clone()
        )

    def with_updates(self, **updates) -> SelectionFilter:
        """ Create a copy of this SelectionFilter and updates the fields """
        data = self.to_dict()
        data.update(updates)
        return SelectionFilter.from_dict(data)

    def to_dict(self) -> dict:
        """ Convert this SelectionFilter to a dictionary. """
        return {
            "sport": self.sport,
            "name": self.name,
            "uid": self.uid,
            "filter_type": self.filter_type.value,
            "fields": self.fields.to_dict() if self.fields else None,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @classmethod
    def from_dict(cls, data: dict) -> SelectionFilter:
        """ Create a SelectionFilter from a dictionary. """
        data = dict(data)
        filter_type = FilterType(data.pop("filter_type"))
        fields_cls = FILTER_TYPE_TO_FIELDS.get(filter_type, EmptyFilterFields)
        fields = fields_cls.from_dict(data.pop("fields", {}))
        return cls(
            filter_type=filter_type,
            fields=fields,
            **data
        )

    @classmethod
    def empty(cls, sport: str, name: str = "") -> SelectionFilter:
        return cls(sport=sport, filter_type=FilterType.EMPTY, name=name)
