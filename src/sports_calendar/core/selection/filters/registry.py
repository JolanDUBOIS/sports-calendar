from typing import Type
from dataclasses import dataclass

from .enums import FilterType, FilterTarget
from .fields import (
    FilterFields,
    EmptyFilterFields,
    MinRankingFilterFields,
    CompetitionsFilterFields,
    TeamsFilterFields,
    SessionsFilterFields
)


# === Filter Definition Data Class ====
@dataclass(frozen=True)
class FilterDefinition:
    fields_cls: Type[FilterFields]
    target: FilterTarget


# === Central Static Registry for Filter Types and Targets ====
FILTER_DEFINITIONS: dict[FilterType, FilterDefinition] = {
    FilterType.EMPTY: FilterDefinition(
        fields_cls=EmptyFilterFields,
        target=FilterTarget.NONE
    ),
    FilterType.MIN_RANKING: FilterDefinition(
        fields_cls=MinRankingFilterFields,
        target=FilterTarget.TEAM
    ),
    FilterType.COMPETITIONS: FilterDefinition(
        fields_cls=CompetitionsFilterFields,
        target=FilterTarget.COMPETITION
    ),
    FilterType.TEAMS: FilterDefinition(
        fields_cls=TeamsFilterFields,
        target=FilterTarget.TEAM
    ),
    FilterType.SESSIONS: FilterDefinition(
        fields_cls=SessionsFilterFields,
        target=FilterTarget.SESSION
    ),
}


# === Mapping from FilterTarget to Basic FilterFields ====
FILTER_TARGET_TO_BASIC: dict[FilterTarget, type[FilterFields]] = {
    FilterTarget.NONE: EmptyFilterFields,
    FilterTarget.TEAM: TeamsFilterFields,
    FilterTarget.COMPETITION: CompetitionsFilterFields,
    FilterTarget.SESSION: SessionsFilterFields,
}