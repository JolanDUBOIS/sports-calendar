from __future__ import annotations
from uuid import uuid4
from datetime import datetime
from dataclasses import dataclass, field

from . import logger
from .fields import FilterFields, EmptyFilterFields
from .definitions import FilterType, FILTER_DEFINITIONS
from sports_calendar.core import SportType
from sports_calendar.core.utils import validate, validate_timestamp


@dataclass(frozen=True)
class SelectionFilter:
    sport: SportType
    name: str = ""
    uid: str = field(default_factory=lambda: str(uuid4())[:8])
    fields: FilterFields = field(default_factory=EmptyFilterFields)
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

    def __post_init__(self):
        validate_timestamp(self.created_at, "created_at", logger)

    @property
    def filter_type(self) -> FilterType:
        return self.fields.filter_type

    def clone(self) -> SelectionFilter:
        """ Create a deep copy of this SelectionFilter with a new ID. """
        return SelectionFilter(
            sport=self.sport,
            name=self.name,
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
            "sport": self.sport.value,
            "name": self.name,
            "uid": self.uid,
            "fields": self.fields.to_dict() if self.fields else None,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SelectionFilter:
        """ Create a SelectionFilter from a dictionary. """
        data = dict(data)

        sport = SportType(data.pop("sport"))

        fields_data = data.pop("fields") or {}

        raw_filter_type = fields_data.get("filter_type", FilterType.EMPTY.value)
        filter_type = FilterType(raw_filter_type)

        fields_cls = FILTER_DEFINITIONS[filter_type].fields_cls
        fields = fields_cls.from_dict(fields_data)

        return cls(
            sport=sport,
            fields=fields,
            **data
        )

    @classmethod
    def empty(cls, sport: SportType, name: str = "") -> SelectionFilter:
        return cls(sport=sport, name=name, fields=EmptyFilterFields())
