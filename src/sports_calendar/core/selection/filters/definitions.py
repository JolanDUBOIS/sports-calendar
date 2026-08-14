from dataclasses import dataclass

from .enums import FilterTarget, FilterType
from .fields import (
    CompetitionsFilterFields,
    CompetitorsFilterFields,
    EmptyFilterFields,
    FilterFields,
    MinRankingFilterFields,
    SessionsFilterFields,
    WorldRankingFilterFields,
)


# === Filter Definition Data Class ====
@dataclass(frozen=True)
class FilterDefinition:
    fields_cls: type[FilterFields]
    target: FilterTarget


# === Central Static Registry for Filter Types and Targets ====
FILTER_DEFINITIONS: dict[FilterType, FilterDefinition] = {
    FilterType.EMPTY: FilterDefinition(
        fields_cls=EmptyFilterFields,
        target=FilterTarget.NONE
    ),
    FilterType.MIN_RANKING: FilterDefinition(
        fields_cls=MinRankingFilterFields,
        target=FilterTarget.COMPETITOR
    ),
    FilterType.WORLD_RANKING: FilterDefinition(
        fields_cls=WorldRankingFilterFields,
        target=FilterTarget.COMPETITOR
    ),
    FilterType.COMPETITIONS: FilterDefinition(
        fields_cls=CompetitionsFilterFields,
        target=FilterTarget.COMPETITION
    ),
    FilterType.COMPETITORS: FilterDefinition(
        fields_cls=CompetitorsFilterFields,
        target=FilterTarget.COMPETITOR
    ),
    FilterType.SESSIONS: FilterDefinition(
        fields_cls=SessionsFilterFields,
        target=FilterTarget.SESSION
    ),
}


# === Mapping from FilterTarget to Basic FilterFields ====
FILTER_TARGET_TO_BASIC: dict[FilterTarget, type[FilterFields]] = {
    FilterTarget.NONE: EmptyFilterFields,
    FilterTarget.COMPETITOR: CompetitorsFilterFields,
    FilterTarget.COMPETITION: CompetitionsFilterFields,
    FilterTarget.SESSION: SessionsFilterFields,
}
