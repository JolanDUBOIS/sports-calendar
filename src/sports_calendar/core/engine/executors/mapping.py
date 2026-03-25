from __future__ import annotations

from sports_calendar.core.selection.filters.registry import FilterType

from .min_ranking import MinRankingExecutor
from .teams import TeamsExecutor
from .sessions import SessionsExecutor
from .competitions import CompetitionsExecutor
from .empty import EmptyExecutor
from .base import BaseExecutor


EXECUTOR_MAP: dict[FilterType, type[BaseExecutor]] = {
    FilterType.MIN_RANKING: MinRankingExecutor,
    FilterType.TEAMS: TeamsExecutor,
    FilterType.SESSIONS: SessionsExecutor,
    FilterType.COMPETITIONS: CompetitionsExecutor,
    FilterType.EMPTY: EmptyExecutor,
}
