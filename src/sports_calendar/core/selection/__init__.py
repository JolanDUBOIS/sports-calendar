import logging
logger = logging.getLogger(__name__)

from .specs import (
    SelectionSpec,
    SelectionItemSpec,
    SelectionFilterSpec,
    MinRankingFilterSpec,
    StageFilterSpec,
    TeamsFilterSpec,
    CompetitionsFilterSpec,
    SessionFilterSpec
)
from .manager import SelectionManager
from .factory import SelectionSpecFactory
from .engine import SelectionApplier