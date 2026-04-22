from __future__ import annotations

from sports_calendar.core.selection.filters.definitions import FilterType

from .min_ranking import MinRankingExecutor
from .competitors import CompetitorsExecutor
from .sessions import SessionsExecutor
from .competitions import CompetitionsExecutor
from .empty import EmptyExecutor
from .base import BaseExecutor


EXECUTOR_MAP: dict[FilterType, type[BaseExecutor]] = {
    FilterType.MIN_RANKING: MinRankingExecutor,
    FilterType.COMPETITORS: CompetitorsExecutor,
    FilterType.SESSIONS: SessionsExecutor,
    FilterType.COMPETITIONS: CompetitionsExecutor,
    FilterType.EMPTY: EmptyExecutor,
}
