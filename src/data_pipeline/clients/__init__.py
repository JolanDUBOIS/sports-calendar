import logging

logger = logging.getLogger(__name__)

from .api import ESPNApiClient, FootballDataApiClient
from .web import LiveSoccerScraper, FootballRankingScraper