import logging
logger = logging.getLogger(__name__)

from .base import BaseTable
from .f1 import F1EventsTable
from .football import (
    FootballRegionsTable,
    FootballCompetitionsTable,
    FootballTeamsTable,
    FootballMatchesTable,
    FootballStandingsTable,
    FootballMatchesManager
)