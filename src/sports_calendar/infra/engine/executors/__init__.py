import logging
logger = logging.getLogger(__name__)

from .base import BaseExecutor
from .competitions import CompetitionsExecutor
from .empty import EmptyExecutor
from .min_ranking import MinRankingExecutor
from .competitors import CompetitorsExecutor
from .sessions import SessionsExecutor
from .mapping import EXECUTOR_MAP