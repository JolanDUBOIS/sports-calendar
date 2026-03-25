import logging
logger = logging.getLogger(__name__)

from .base import SportsEvent, SportsEventCollection
from .f1 import F1Event
from .football import FootballEvent