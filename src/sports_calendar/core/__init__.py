import logging
logger = logging.getLogger(__name__)

from .competition_stages import CompetitionStage
from ..infra.config import Paths, setup_logging
from .sports import SportType