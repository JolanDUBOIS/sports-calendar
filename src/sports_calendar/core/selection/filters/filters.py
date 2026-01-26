from __future__ import annotations
import logging
from enum import Enum
from uuid import uuid4
from typing import Callable, Any
from dataclasses import dataclass, field

from . import logger
from .specs import FILTER_SPECS
from .codecs import FieldCodec, IdentityCodec, EnumCodec
from ...utils import validate
from ...db.schemas import SPORT_SCHEMAS
from ...competition_stages import CompetitionStage


class FilterType(Enum):
    EMPTY = "empty"
    MIN_RANKING = "min_ranking"
    STAGE = "stage"
    TEAMS = "teams"
    COMPETITIONS = "competitions"
    SESSION = "session"

    def _get_spec(self):
        return next((spec for spec in FILTER_SPECS if spec.filter_type == self.value), None)

    def specific_fields(self) -> list[str]:
        """ Get all fields specific to this filter type. """
        spec = self._get_spec()
        return spec.fields if spec else []

    def fields(self) -> list[str]:
        """ Get all fields for this filter type, including common ones. """
        return ["sport", "uid", "filter_type", "name"] + self.specific_fields()

    def validators(self) -> list[Callable[[dict[str, Any], logging.Logger], None]]:
        """ Get all validators for this filter type. """
        spec = self._get_spec()
        return spec.validators if spec else []

    def valid_values(self, field: str) -> set[Any] | None:
        """ Get valid values for a specific field of this filter type. """
        spec = self._get_spec()
        if spec and spec.valid_values:
            return spec.valid_values.get(field, None)
        return None


FIELD_CODECS: dict[str, FieldCodec[Any]] = {
    "sport": IdentityCodec(),
    "filter_type": EnumCodec(FilterType),
    "uid": IdentityCodec(),
    "name": IdentityCodec(),

    "rule": IdentityCodec(),
    "ranking": IdentityCodec(),
    "competition_ids": IdentityCodec(),
    "reference_team": IdentityCodec(),
    "stage": EnumCodec(CompetitionStage),
    "team_ids": IdentityCodec(),
    "sessions": IdentityCodec(),
}


@dataclass
class SelectionFilter:
    """ A filter criterion for selecting matches/events. """
    sport: str  # Denormalized from parent SelectionItem
    filter_type: FilterType
    uid: str = field(default_factory=lambda: str(uuid4())[:8])
    name: str = ""

    # All possible filter parameters (only some will be used depending on filter_type)
    rule: str | None = None
    ranking: int | None = None
    competition_ids: list[int] | None = None
    reference_team: str | None = None
    stage: CompetitionStage | None = None
    team_ids: list[int] | None = None
    sessions: list[str] | None = None

    def __post_init__(self):
        validate(self.sport in SPORT_SCHEMAS, f"Invalid sport: {self.sport}", logger)
        self._validate_filter_data()

    def _validate_filter_data(self):
        """ Validate based on filter specification. """
        validators = self.filter_type.validators()
        data = {field: getattr(self, field) for field in self.filter_type.specific_fields()}

        for validator in validators:
            validator(data, logger)

        for field in self.__dataclass_fields__:
            if field not in self.filter_type.fields():
                validate(getattr(self, field) is None, f"Field '{field}' must be None for filter type '{self.filter_type.value}'", logger)

    @property
    def fields(self) -> list[str]:
        """ Get all fields for this SelectionFilter. """
        return self.filter_type.fields()

    def clone(self) -> SelectionFilter:
        """ Create a deep copy of this SelectionFilter with a new ID. """
        data = {f: getattr(self, f) for f in self.fields}
        return type(self)(**{f: data[f] for f in data if f != "uid"})

    def with_updates(self, **updates) -> SelectionFilter:
        """ Create a copy of this SelectionFilter and updates the fields """
        data = self.to_dict()
        data.update(updates)
        return SelectionFilter.from_dict(data)

    def to_dict(self) -> dict:
        """ Convert this SelectionFilter to a dictionary. """
        return {
            f: FIELD_CODECS[f].to_dict(getattr(self, f))
            for f in self.fields
        }

    @classmethod
    def from_dict(cls, data: dict) -> SelectionFilter:
        """ Create a SelectionFilter from a dictionary. """
        return cls(**{
            f: FIELD_CODECS[f].from_dict(data.get(f))
            for f in FIELD_CODECS
            if f in data
        })

    @classmethod
    def empty(cls, sport: str) -> SelectionFilter:
        return cls(sport=sport, filter_type=FilterType.EMPTY)
