from __future__ import annotations

from typing import TYPE_CHECKING

from sports_calendar.core.selection.filters.definitions import FilterType

from .competitions import CompetitionsExecutor
from .competitors import CompetitorsExecutor
from .empty import EmptyExecutor
from .min_ranking import MinRankingExecutor
from .sessions import SessionsExecutor
from .world_ranking import WorldRankingExecutor

if TYPE_CHECKING:
    from .base import BaseExecutor


EXECUTOR_MAP: dict[FilterType, type[BaseExecutor]] = {
    FilterType.MIN_RANKING: MinRankingExecutor,
    FilterType.WORLD_RANKING: WorldRankingExecutor,
    FilterType.COMPETITORS: CompetitorsExecutor,
    FilterType.SESSIONS: SessionsExecutor,
    FilterType.COMPETITIONS: CompetitionsExecutor,
    FilterType.EMPTY: EmptyExecutor,
}
